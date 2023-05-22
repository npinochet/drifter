package utxo

// modified from https://github.com/theStack/utxo_dump_tools/blob/master/utxo_to_sqlite/utxo_to_sqlite.go

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/npinochet/drifter/dht"
)

const (
	HashSize        = 20
	PubKeySize      = 33
	WitnessProgSize = 32

	filterProb = 0.001
)

type TxType byte

const (
	UNKNOWN TxType = iota
	P2PK
	P2PKU
	P2PKH
	P2SH
	P2WPKH
	P2WSH
	P2TR
)

type ScriptType struct {
	bucket []byte
	filter *bloom.BloomFilter
}

var (
	hashBucket        = []byte("H")
	pubKeyBucket      = []byte("P")
	witnessProgBucket = []byte("W")
	ErrUidentified    = errors.New("could not identify script")
	db                *dht.DB

	HashType        = &ScriptType{hashBucket, nil}
	PubKeyType      = &ScriptType{pubKeyBucket, nil}
	WitnessProgType = &ScriptType{witnessProgBucket, nil}
)

func (t TxType) String() string {
	switch t {
	case P2PK:
		return "P2PK"
	case P2PKU:
		return "P2PKU"
	case P2PKH:
		return "P2PKH"
	case P2SH:
		return "P2SH"
	case P2WPKH:
		return "P2WPKH"
	case P2WSH:
		return "P2WSH"
	case P2TR:
		return "P2TR"
	}
	return "UNKNOWN"
}

func ReadUTXOFile(inputFileName, lookupTableName string, indexBitSize int64) {
	f, err := os.OpenFile(inputFileName, os.O_RDONLY, 0644)
	if err != nil {
		panic(err)
	}
	create := false
	if _, err = os.Stat(lookupTableName); errors.Is(err, os.ErrNotExist) {
		create = true
	} else if err != nil {
		panic(err)
	}
	opts := &dht.Options{KByteSize: PubKeySize + 1, VByteSize: 1, IndexBitSize: indexBitSize, Create: create}
	if db, err = dht.Open(lookupTableName, opts); err != nil {
		panic(err)
	}

	utxof := bufio.NewReader(f)
	// read metadata
	var blockHash [32]byte
	var numUTXOs uint64
	readIntoSlice(utxof, blockHash[:])
	read(utxof, &numUTXOs)
	log.Printf("UTXO Snapshot at block %s, contains %d txs\n", hashToStr(blockHash), numUTXOs)

	HashType.filter = bloom.NewWithEstimates(uint(numUTXOs), filterProb)
	PubKeyType.filter = bloom.NewWithEstimates(uint(numUTXOs), filterProb)
	WitnessProgType.filter = bloom.NewWithEstimates(uint(numUTXOs), filterProb)

	batch := db.NewBatch()
	var unidentifiedScripts, hashN, pubKeyN, witnessProgN uint
	for coin_idx := uint64(1); coin_idx <= numUTXOs; coin_idx++ {
		_, _ = utxof.Discard(32) // readIntoSlice(utxof, prevoutHash[:]) // read 32 bytes
		_, _ = utxof.Discard(4)  // read(utxof, &prevoutIndex)   // read Uint32
		_ = readUvarint(utxof)   // read code VARINT
		_ = readUvarint(utxof)   // read amount VARINT

		if coin_idx%(1024*1024) == 0 {
			if err := batch.Commit(); err != nil {
				panic(err)
			}
			log.Printf("%d txs read [%.2f%%]\n", coin_idx, (float64(coin_idx)/float64(numUTXOs))*100)
		}

		partialScriptHash, txType, err := readCompressedScript(utxof)
		if err != nil && !errors.Is(err, ErrUidentified) {
			log.Println("Error decompressing script:", err.Error())
		}
		var scriptType *ScriptType
		switch txType {
		case P2PKH, P2SH, P2WPKH:
			scriptType = HashType
			hashN++
		case P2PK, P2PKU:
			scriptType = PubKeyType
			pubKeyN++
		case P2WSH:
			scriptType = WitnessProgType
			witnessProgN++
		default:
			unidentifiedScripts++
			continue
		}
		scriptType.filter.Add(partialScriptHash)
		if !create {
			continue
		}
		if err := batch.Add(append(scriptType.bucket, partialScriptHash...), []byte{byte(txType)}); err != nil {
			panic(err)
		}
	}
	if err := batch.Commit(); err != nil {
		panic(err)
	}

	log.Printf("Hashes (P2PKH, P2SH, P2WPKH): %d [%.2f%%]\n", hashN, float64(hashN)/float64(numUTXOs)*100)
	log.Printf("PubKeys (P2PK): %d [%.2f%%]\n", pubKeyN, float64(pubKeyN)/float64(numUTXOs)*100)
	log.Printf("WitnessProgs (P2WSH): %d [%.2f%%]\n", witnessProgN, float64(witnessProgN)/float64(numUTXOs)*100)
	log.Printf("Unidentified scripts: %d [%.2f%%]\n", unidentifiedScripts, float64(unidentifiedScripts)/float64(numUTXOs)*100)
}

func CheckDataExistance(partialScriptHash []byte, scriptType *ScriptType) (TxType, bool) {
	if scriptType == nil || !scriptType.filter.Test(partialScriptHash) {
		return UNKNOWN, false
	}

	value, err := db.Get(append(scriptType.bucket, partialScriptHash...))
	if err != nil {
		panic(err)
	}
	if len(value) == 0 {
		return UNKNOWN, false
	}

	return TxType(value[0]), true
}

func GetBiggestCollisionDepth() uint64 { return db.BiggestCollisionDepth.Load() }

func readCompressedScript(r *bufio.Reader) ([]byte, TxType, error) {
	spkSize := readUvarint(r)
	switch spkSize {
	case 0: // P2PKH
		hash := make([]byte, HashSize)
		readIntoSlice(r, hash)
		return hash, P2PKH, nil
	case 1: // P2SH
		hash := make([]byte, HashSize)
		readIntoSlice(r, hash)
		return hash, P2SH, nil
	case 2, 3: // P2PK (compressed)
		pubKey := make([]byte, PubKeySize)
		pubKey[0] = byte(spkSize)
		readIntoSlice(r, pubKey[1:])
		return pubKey, P2PK, nil
	case 4, 5: // P2PK (uncompressed)
		pubKey := make([]byte, PubKeySize)
		pubKey[0] = byte(spkSize) - 2
		readIntoSlice(r, pubKey[1:])
		return pubKey, P2PKU, nil
	default: // others (bare multisig, segwit etc.)
		readSize := spkSize - 6
		if readSize > 10000 {
			return nil, UNKNOWN, fmt.Errorf("too long script with size %d", readSize)
		}
		buf := make([]byte, readSize)
		readIntoSlice(r, buf)
		// Segwit
		if readSize < 4 || readSize > 42 {
			break
		}
		if buf[0] != 0 && (buf[0] < 0x51 || buf[0] > 0x60) {
			break
		}
		if buf[1]+2 != byte(readSize) {
			break
		}
		witnessVersion, witnessProg := buf[0], buf[2:]
		if witnessVersion == 0 {
			if len(witnessProg) == 20 {
				return witnessProg, P2WPKH, nil
			}
			if len(witnessProg) == 32 {
				return witnessProg, P2WSH, nil
			}
		}
		if witnessVersion == 1 && len(witnessProg) == 32 {
			return witnessProg, P2TR, nil
		}
	}

	return nil, UNKNOWN, fmt.Errorf("%w with size %d", ErrUidentified, spkSize-6)
}

func readIntoSlice(r *bufio.Reader, buf []byte) {
	if _, err := io.ReadFull(r, buf); err != nil {
		panic(err)
	}
}

func read[T any](r *bufio.Reader, target T) {
	if err := binary.Read(r, binary.LittleEndian, target); err != nil {
		panic(err)
	}
}

func readUvarint(r *bufio.Reader) uint64 {
	var n uint64
	for {
		dat, _ := r.ReadByte()
		n = (n << 7) | uint64(dat&0x7f)
		if (dat & 0x80) > 0 {
			n++
		} else {
			return n
		}
	}
}

func hashToStr(bytes [32]byte) string {
	for i, j := 0, 31; i < j; i, j = i+1, j-1 {
		bytes[i], bytes[j] = bytes[j], bytes[i]
	}
	return fmt.Sprintf("%x", bytes)
}
