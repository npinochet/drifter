package dht

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

func BenchmarkWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var small int64 = 21 // 8 MB
		if err := WriteAppendBatchBench(small); err != nil {
			panic(err)
		}
		var big int64 = 31 // 8.5 GB
		if err := WriteAppendBatchBench(big); err != nil {
			panic(err)
		}
	}
}

func WriteAppendBench(indexBitSize int64) error {
	start := time.Now()

	fileName := fmt.Sprintf("test_%d.dht", indexBitSize)
	opts := &Options{KByteSize: 50, VByteSize: 1, Create: true, IndexBitSize: indexBitSize}
	db, err := Open(fileName, opts)
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
	os.Remove(fileName)

	return nil
}

func WriteAppendBatchBench(indexBitSize int64) error {
	start := time.Now()

	fileName := fmt.Sprintf("test_%d.dht", indexBitSize)
	opts := &Options{KByteSize: 50, VByteSize: 1, Create: true, IndexBitSize: indexBitSize}
	db, err := Open(fileName, opts)
	if err != nil {
		return err
	}
	defer db.Close()
	batch := db.NewBatch()
	for i := 0; i < 100000; i++ {
		buf := make([]byte, 50)
		_, _ = rand.Read(buf)
		batch.Add(buf, []byte{1})
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	log.Printf("Time lasped with size %d: %s\n", indexBitSize, time.Since(start).String())
	os.Remove(fileName)

	return nil
}
