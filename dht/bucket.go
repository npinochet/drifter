package dht

import (
	"encoding/binary"
)

type bucket struct {
	key     []byte
	val     []byte
	nextOff uint64
}

func (b *bucket) MarshalBinary(kLen, vLen int) []byte {
	buf := make([]byte, kLen+vLen+IndexEntryByteSize)
	copy(buf, b.key)
	copy(buf[kLen:], b.val)
	binary.LittleEndian.PutUint64(buf[kLen+vLen:], b.nextOff)

	return buf
}

type bucketHandler struct {
	db     *DB
	boff   uint64
	bucket bucket
}

func (bh *bucketHandler) Read() error {
	if bh.boff == 0 {
		return nil
	}
	off := int64(uint64(bh.db.idxSize) + bh.boff - 1)
	bucketBuf := make([]byte, bh.db.kLen+bh.db.vLen+IndexEntryByteSize)
	if _, err := bh.db.f.ReadAt(bucketBuf, off); err != nil {
		return err
	}
	key := bucketBuf[:bh.db.kLen]
	val := bucketBuf[bh.db.kLen : bh.db.kLen+bh.db.vLen] // this errors
	nextOffBuf := binary.LittleEndian.Uint64(bucketBuf[bh.db.kLen+bh.db.vLen:])

	bh.bucket = bucket{key, val, nextOffBuf}

	return nil
}
