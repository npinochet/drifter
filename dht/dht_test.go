package dht

import (
	"bytes"
	"os"
	"testing"
)

const fileName = "test.dht"

var opts = &Options{
	Create:       true,
	IndexBitSize: 2,
}

func TestPutGet(t *testing.T) {
	db, err := Open(fileName, opts)
	if err != nil {
		t.Error(err)
	}
	key := []byte("key")
	val := []byte{1, 2, 3}
	if err := db.Put(key, val); err != nil {
		t.Error(err)
	}
	if err := db.Put([]byte("key2"), []byte("val")); err != nil {
		t.Error(err)
	}
	valret, err := db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(valret, val) {
		t.Errorf("Get with same key must return same value: original: %v, getted: %v", valret, val)
	}
	os.Remove(fileName)
}
func TestPutCollision(t *testing.T) {
	db, err := Open(fileName, opts)
	if err != nil {
		t.Error(err)
	}

	key := []byte("key")
	val := []byte{1, 2, 3}
	if err := db.Put(key, val); err != nil {
		t.Error(err)
	}
	for i := 0; i < 100; i++ {
		keyval := []byte{byte(i)}
		if err := db.Put(keyval, keyval); err != nil {
			t.Error(err)
		}
	}
	valret, err := db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(valret, val) {
		t.Errorf("Get with same key must return same value: original: %v, getted: %v", valret, val)
	}
	os.Remove(fileName)
}

func TestBatch(t *testing.T) {
	db, err := Open(fileName, opts)
	if err != nil {
		t.Error(err)
	}
	key := []byte("key")
	val := []byte{1, 2, 3}
	batch := db.NewBatch()
	batch.Add(key, val)
	if err := batch.Commit(); err != nil {
		t.Error(err)
	}
	valret, err := db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(valret, val) {
		t.Errorf("Get with same key must return same value: original: %v, getted: %v", valret, val)
	}
	os.Remove(fileName)
}

func TestBatchCollision(t *testing.T) {
	db, err := Open(fileName, opts)
	if err != nil {
		t.Error(err)
	}

	key := []byte("key")
	val := []byte{1, 2, 3}
	batch := db.NewBatch()
	batch.Add(key, val)
	for i := 0; i < 100; i++ {
		keyval := []byte{byte(i)}
		batch.Add(keyval, keyval)
	}
	if err := batch.Commit(); err != nil {
		t.Error(err)
	}
	valret, err := db.Get(key)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(valret, val) {
		t.Errorf("Get with same key must return same value: original: %v, getted: %v", valret, val)
	}
	os.Remove(fileName)
}
