package dht

import (
	"crypto/rand"
	"fmt"
	"log"
	"testing"
	"time"
)

func BenchmarkWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var small int64 = 21 // 16 MB
		if err := WriteAppendBatchBench(small); err != nil {
			panic(err)
		}
		var big int64 = 31 // 17 GB
		if err := WriteAppendBatchBench(big); err != nil {
			panic(err)
		}
	}
}

func WriteAppendBench(indexBitSize int64) error {
	start := time.Now()

	opts := &Options{KByteSize: 50, VByteSize: 1, Create: true, IndexBitSize: indexBitSize}
	db, err := Open(fmt.Sprintf("test_%d.dht", indexBitSize), opts)
	if err != nil {
		return err
	}
	defer db.Close()
	for i := 0; i < 100000; i++ {
		buf := make([]byte, 50)
		_, _ = rand.Read(buf)
		if err := db.Put(buf, []byte{1}); err != nil {
			return err
		}
	}
	log.Printf("Time lasped with size %d: %s\n", indexBitSize, time.Since(start).String())

	return nil
}

func WriteAppendBatchBench(indexBitSize int64) error {
	start := time.Now()

	opts := &Options{KByteSize: 50, VByteSize: 1, Create: true, IndexBitSize: indexBitSize}
	db, err := Open(fmt.Sprintf("test_%d.dht", indexBitSize), opts)
	if err != nil {
		return err
	}
	defer db.Close()
	batch := db.NewBatch()
	for i := 0; i < 100000; i++ {
		buf := make([]byte, 50)
		_, _ = rand.Read(buf)
		if err := batch.Add(buf, []byte{1}); err != nil {
			return err
		}
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	log.Printf("Time lasped with size %d: %s\n", indexBitSize, time.Since(start).String())

	return nil
}
