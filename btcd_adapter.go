package main

import (
	"fmt"
	"path/filepath"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	chain        = &chaincfg.MainNetParams
	addrIndexKey = []byte("txbyaddridx")
	dbPath       = filepath.Join("btcd", "data", chain.Name, "blocks_ffldb")
	db           database.DB
)

func init() {
	//var err error
	//ReadOnly: true
	//if db, err = database.Open("ffldb", dbPath, chain.Net); err != nil {
	//	log.Fatal(err)
	//}
}

func fastHasTxs(addrHash []byte) (bool, error) {
	opts := opt.Options{
		ErrorIfExist: false,
		Strict:       opt.DefaultStrict,
		Compression:  opt.NoCompression,
		Filter:       filter.NewBloomFilter(10),
		ReadOnly:     true,
		NoSync:       true,
	}
	ldb, err := leveldb.OpenFile(filepath.Join(dbPath, "metadata"), &opts)

	var addrKey [22]byte
	copy(addrKey[1:], addrHash[:])
	val, err := ldb.Get(addrIndexKey, nil)
	fmt.Println(val, err)
	//leveldb.Snapshot

	return false, err
}

func hasTxs(addrHash []byte) (bool, error) {
	var hasTxs bool
	err := db.View(func(dbTx database.Tx) error {
		addrIdxBucket := dbTx.Metadata().Bucket(addrIndexKey)
		var addrKey [22]byte
		copy(addrKey[1:], addrHash[:])
		hasTxs = addrIdxBucket.Get(addrKey[:]) != nil

		return nil
	})

	return hasTxs, err
}
