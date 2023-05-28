package dht

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
)

const (
	OffsetEntryByteSize   = 4
	CommitWriteBufferSize = 65536 // 64 KiB

	hashBitSize              = 32
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
	mutex                 sync.RWMutex
}

type Batch struct {
	db      *DB
	buckets []*bucket
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

	idxSize := int64(1<<idxBitSize) * OffsetEntryByteSize
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
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	b, err := db.getBucket(db.hash(key), key)
	if b == nil {
		return nil, err
	}

	return b.val, err
}

func (db *DB) Put(key, val []byte) error {
	kLen, vLen := len(key), len(val)
	if kLen > db.kLen {
		return ErrKLenTooBig
	}
	if vLen > db.vLen {
		return ErrVLenTooBig
	}
	bucket := &bucket{kLen: uint16(kLen), vLen: uint16(vLen), key: key, val: val}
	db.mutex.Lock()
	defer db.mutex.Unlock()

	return db.putBucket(db.hash(key), bucket)
}

func (db *DB) NewBatch() *Batch { return &Batch{db: db} }

func (b *Batch) Add(key, val []byte) {
	bucket := &bucket{kLen: uint16(len(key)), vLen: uint16(len(val)), key: key, val: val}
	b.buckets = append(b.buckets, bucket)
}

func (b *Batch) Commit() error {
	if len(b.buckets) == 0 {
		return nil
	}
	b.db.mutex.Lock()
	defer b.db.mutex.Unlock()

	bucketSize := b.db.bucketSize()
	boffStart := (b.db.size-b.db.idxSize)/int64(bucketSize) + 1
	indexCache := map[int64]uint32{}
	ioffs := make([]int64, len(b.buckets))
	buckets := make([]byte, bucketSize*len(b.buckets))

	for i, bucket := range b.buckets {
		ioff := int64(b.db.hash(bucket.key)) * OffsetEntryByteSize
		nextOff, ok := indexCache[ioff]
		if !ok {
			var err error
			if nextOff, err = b.db.readIndexOffset(ioff); err != nil {
				return err
			}
		}
		indexCache[ioff] = uint32(boffStart + int64(i))
		bucket.nextOff = nextOff

		ioffs[i] = ioff
		copy(buckets[i*bucketSize:], b.db.MarshalBinary(bucket))
	}
	if err := b.db.append(buckets); err != nil {
		return err
	}
	b.buckets = nil

	return b.db.commitWriteIndex(ioffs, indexCache)
}

func (db *DB) getBucket(hash uint32, key []byte) (*bucket, error) {
	ioff := int64(hash) * OffsetEntryByteSize
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

func (db *DB) putBucket(hash uint32, newBucket *bucket) error {
	newBoff := (db.size-db.idxSize)/int64(db.bucketSize()) + 1
	newBoffBuf := make([]byte, OffsetEntryByteSize)
	binary.LittleEndian.PutUint32(newBoffBuf, uint32(newBoff))

	ioff := int64(hash) * OffsetEntryByteSize
	var err error
	newBucket.nextOff, err = db.readIndexOffset(ioff)
	if err != nil {
		return err
	}
	if _, err := db.f.WriteAt(newBoffBuf, ioff); err != nil {
		return err
	}

	return db.append(db.MarshalBinary(newBucket))
}

func (db *DB) commitWriteIndex(ioffs []int64, indexCache map[int64]uint32) error {
	sort.Slice(ioffs, func(i, j int) bool { return ioffs[i] < ioffs[j] })

	var curIoffI = 0
	var prevIoff int64 = -1
	writeBuffer := make([]byte, CommitWriteBufferSize)
	for i := int64(0); i < db.idxSize; i += CommitWriteBufferSize {
		if ioffs[curIoffI] >= i+CommitWriteBufferSize {
			continue
		}
		if i+CommitWriteBufferSize > db.idxSize {
			writeBuffer = writeBuffer[:db.idxSize-i]
		}
		if _, err := db.f.ReadAt(writeBuffer, i); err != nil {
			return err
		}
		for curIoffI < len(ioffs) && ioffs[curIoffI] < i+CommitWriteBufferSize {
			if prevIoff == ioffs[curIoffI] {
				curIoffI++
				continue
			}
			prevIoff = ioffs[curIoffI]
			boffBuf := make([]byte, OffsetEntryByteSize)
			binary.LittleEndian.PutUint32(boffBuf, indexCache[ioffs[curIoffI]])
			copy(writeBuffer[ioffs[curIoffI]-i:], boffBuf)
			curIoffI++
		}
		if _, err := db.f.WriteAt(writeBuffer, i); err != nil {
			return err
		}
		if curIoffI >= len(ioffs)-1 {
			break
		}
	}

	return nil
}

func (db *DB) readIndexOffset(off int64) (uint32, error) {
	buf := make([]byte, OffsetEntryByteSize)
	if _, err := db.f.ReadAt(buf, off); err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(buf), nil
}

func (db *DB) append(data []byte) error {
	writen, err := db.f.WriteAt(data, db.size)
	if err != nil {
		return err
	}
	db.size += int64(writen)

	return nil
}

func (db *DB) hash(key []byte) uint32 {
	return uint32(xxhash.Sum64(key) & ((1 << db.idxBitSize) - 1))
}
