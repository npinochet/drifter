package dht

import (
	"encoding/binary"
)

type bucket struct {
	kLen, vLen uint16
	key, val   []byte
	nextOff    uint32
}

func (db *DB) bucketSize() int { return db.kLen + db.vLen + OffsetEntryByteSize*2 }

func (db *DB) MarshalBinary(b *bucket) []byte {
	buf := make([]byte, db.bucketSize())
	binary.LittleEndian.PutUint16(buf, b.kLen)
	off := OffsetEntryByteSize / 2
	binary.LittleEndian.PutUint16(buf[off:], b.vLen)
	off += OffsetEntryByteSize / 2
	copy(buf[off:], b.key)
	off += db.kLen
	copy(buf[off:], b.val)
	off += db.vLen
	binary.LittleEndian.PutUint32(buf[off:], b.nextOff)

	return buf
}

type bucketHandler struct {
	db     *DB
	boff   uint32
	bucket bucket
}

func (bh *bucketHandler) Read() error {
	if bh.boff == 0 {
		return nil
	}
	off := bh.db.idxSize + int64(bh.boff-1)*int64(bh.db.bucketSize())
	bucketBuf := make([]byte, bh.db.bucketSize())
	if _, err := bh.db.f.ReadAt(bucketBuf, off); err != nil {
		return err
	}
	kLen := binary.LittleEndian.Uint16(bucketBuf)
	off = OffsetEntryByteSize / 2
	vLen := binary.LittleEndian.Uint16(bucketBuf[off:])
	off += OffsetEntryByteSize / 2
	key := bucketBuf[off : off+int64(kLen)]
	off += int64(bh.db.kLen)
	val := bucketBuf[off : off+int64(vLen)]
	off += int64(bh.db.vLen)
	nextOffBuf := binary.LittleEndian.Uint32(bucketBuf[off:])
	bh.bucket = bucket{kLen, vLen, key, val, nextOffBuf}

	return nil
}
