package dht

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"sort"
	"sync/atomic"

	"github.com/cespare/xxhash"
)

const (
	IndexEntryByteSize = 8

	hashBitSize              = 64
	defaultFilePerm          = 0644
	defaultIndexBitSize      = 28
	defaultKLen, defaultVLen = 8, 16
)

var (
	ErrKLenTooBig    = errors.New("key byte length bigger than initial configuration")
	ErrVLenTooBig    = errors.New("value byte length bigger than initial configuration")
	ErrIdxSizeTooBig = errors.New("index bit size is too big for the current hash function")
)

type Options struct {
	KByteSize, VByteSize int
	IndexBitSize         int64
	Create               bool
	Perm                 fs.FileMode
}

type DB struct {
	f                     *os.File
	BiggestCollisionDepth atomic.Uint64
	size                  int64
	kLen, vLen            int
	idxBitSize, idxSize   int64
}

type Batch struct {
	db         *DB
	ioffs      []int64
	buckets    []byte
	indexCache map[int64]uint64
}

func Open(name string, opts *Options) (*DB, error) {
	flag := os.O_RDWR
	if opts.Create {
		flag |= os.O_CREATE
	}
	perm := opts.Perm
	if perm == 0 {
		perm = defaultFilePerm
	}
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	idxBitSize := opts.IndexBitSize
	if idxBitSize == 0 {
		idxBitSize = defaultIndexBitSize
	}
	if idxBitSize > hashBitSize {
		return nil, ErrIdxSizeTooBig
	}

	idxSize := int64(1<<idxBitSize) * IndexEntryByteSize
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	if size == 0 {
		size = idxSize
		if err := f.Truncate(size); err != nil {
			return nil, err
		}
	}
	kLen, vLen := opts.KByteSize, opts.VByteSize
	if kLen == 0 {
		kLen = defaultKLen
	}
	if vLen == 0 {
		vLen = defaultVLen
	}

	return &DB{f: f, size: size, kLen: kLen, vLen: vLen, idxBitSize: idxBitSize, idxSize: idxSize}, err
}

func (db *DB) Close() error { return db.f.Close() }

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) > db.kLen {
		return nil, ErrKLenTooBig
	}
	b, err := db.getBucket(db.hash(key), pad(key, db.kLen))
	if b == nil {
		return nil, err
	}
	const zero = "\x00"

	return bytes.TrimRight(b.val, zero), err
}

func (db *DB) Put(key, val []byte) error {
	if len(key) > db.kLen {
		return ErrKLenTooBig
	}
	if len(val) > db.vLen {
		return ErrVLenTooBig
	}
	bucket := &bucket{key: key, val: val}

	return db.putBucket(db.hash(key), bucket)
}

func (db *DB) NewBatch() *Batch { return &Batch{db: db, indexCache: map[int64]uint64{}} }

func (b *Batch) Add(key, val []byte) error {
	ioff := int64(b.db.hash(key)) * IndexEntryByteSize
	nextOff, ok := b.indexCache[ioff]
	if !ok {
		var err error
		nextOff, err = b.db.readIndexOffset(ioff)
		if err != nil {
			return err
		}
	}
	bucketsSize := int64(len(b.ioffs) * (b.db.kLen + b.db.vLen + IndexEntryByteSize))
	b.indexCache[ioff] = uint64(b.db.size - b.db.idxSize + 1 + bucketsSize)
	b.ioffs = append(b.ioffs, ioff)
	bucket := bucket{key: key, val: val, nextOff: nextOff}
	b.buckets = append(b.buckets, bucket.MarshalBinary(b.db.kLen, b.db.vLen)...)

	return nil
}

func (b *Batch) Commit() error {
	if len(b.buckets) == 0 {
		return nil
	}
	if err := b.db.append(b.buckets); err != nil {
		return err
	}

	sort.Slice(b.ioffs, func(i, j int) bool { return b.ioffs[i] < b.ioffs[j] })
	for _, ioff := range b.ioffs {
		boff := b.indexCache[ioff]
		boffBuf := make([]byte, IndexEntryByteSize)
		binary.LittleEndian.PutUint64(boffBuf, boff)
		if _, err := b.db.f.WriteAt(boffBuf, ioff); err != nil {
			return err
		}
	}

	b.buckets = nil
	b.ioffs = nil
	b.indexCache = map[int64]uint64{}

	return nil
}

func (db *DB) getBucket(hash uint64, key []byte) (*bucket, error) {
	ioff := int64(hash) * IndexEntryByteSize
	boff, err := db.readIndexOffset(ioff)
	if boff == 0 {
		return nil, err
	}
	bh := &bucketHandler{db: db, boff: boff}
	if err := bh.Read(); err != nil {
		return nil, err
	}
	var depth uint64
	for bh.boff != 0 {
		depth++
		if depth > db.BiggestCollisionDepth.Load() {
			db.BiggestCollisionDepth.Store(depth)
		}
		if bytes.Equal(bh.bucket.key, key) {
			return &bh.bucket, nil
		}
		bh.boff = bh.bucket.nextOff
		if err := bh.Read(); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func (db *DB) putBucket(hash uint64, newBucket *bucket) error {
	newBoff := db.size - db.idxSize + 1
	newBoffBuf := make([]byte, IndexEntryByteSize)
	binary.LittleEndian.PutUint64(newBoffBuf, uint64(newBoff))

	ioff := int64(hash) * IndexEntryByteSize
	var err error
	newBucket.nextOff, err = db.readIndexOffset(ioff)
	if err != nil {
		return err
	}
	if _, err := db.f.WriteAt(newBoffBuf, ioff); err != nil {
		return err
	}

	return db.append(newBucket.MarshalBinary(db.kLen, db.vLen))
}

func (db *DB) readIndexOffset(off int64) (uint64, error) {
	buf := make([]byte, IndexEntryByteSize)
	if _, err := db.f.ReadAt(buf, off); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(buf), nil
}

func (db *DB) append(data []byte) error {
	writen, err := db.f.WriteAt(data, db.size)
	if err != nil {
		return err
	}
	db.size += int64(writen)

	return nil
}

func (db *DB) hash(key []byte) uint64 {
	return xxhash.Sum64(key) & ((1 << db.idxBitSize) - 1)
}

func pad(data []byte, length int) []byte {
	if len(data) == length {
		return data
	}
	buf := make([]byte, length)
	copy(buf, data)

	return buf
}
