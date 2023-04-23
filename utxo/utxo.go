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
)

const (
	HashSize        = 20
	WitnessProgSize = 32
	PubKeySize      = 33
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

var errUidentified = errors.New("could not identify script")

var (
	hashes       = map[[HashSize]byte]TxType{}
	pubKeys      = map[[PubKeySize]byte]TxType{}
	witnessProgs = map[[WitnessProgSize]byte]TxType{}
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

func readCompressedScript(r *bufio.Reader) ([]byte, TxType, error) {
	spkSize := readUvarint(r)
	switch spkSize {
	case 0: // P2PKH
		hash := [HashSize]byte{}
		readIntoSlice(r, hash[:])
		return hash[:], P2PKH, nil
	case 1: // P2SH
		hash := [HashSize]byte{}
		readIntoSlice(r, hash[:])
		return hash[:], P2SH, nil
	case 2, 3: // P2PK (compressed)
		pubKey := [PubKeySize]byte{byte(spkSize)}
		readIntoSlice(r, pubKey[1:])
		return pubKey[:], P2PK, nil
	case 4, 5: // P2PK (uncompressed)
		pubKey := [PubKeySize]byte{byte(spkSize) - 2}
		readIntoSlice(r, pubKey[1:])
		return pubKey[:], P2PKU, nil
	default: // others (bare multisig, segwit etc.)
		readSize := spkSize - 6
		if readSize > 10000 {
			return nil, UNKNOWN, fmt.Errorf("too long script with size %d", readSize)
		}
		buf := make([]byte, readSize)
		readIntoSlice(r, buf[:])
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
		if witnessVersion == 0 && len(witnessProg) == 20 {
			return witnessProg, P2WPKH, nil
		}
		if witnessVersion == 0 && len(witnessProg) == 32 {
			return witnessProg, P2WSH, nil
		}
		if witnessVersion == 1 && len(witnessProg) == 32 {
			return witnessProg, P2TR, nil
		}
	}

	return nil, UNKNOWN, fmt.Errorf("%w with size %d", errUidentified, spkSize-6)
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

func ReadUTXOFile(inputFilename string) {
	if len(os.Args) != 2 {
		fmt.Println("Must input a valid bitcoin RPC 'dumptxoutset' utxo file")
		os.Exit(1)
	}
	f, err := os.OpenFile(inputFilename, os.O_RDONLY, 0600)
	if err != nil {
		panic(err)
	}
	utxof := bufio.NewReader(f)

	// read metadata
	var blockHash [32]byte
	var numUTXOs uint64
	readIntoSlice(utxof, blockHash[:])
	read(utxof, &numUTXOs)
	log.Printf("UTXO Snapshot at block %s, contains %d coins\n", hashToStr(blockHash), numUTXOs)

	var unidentifiedScripts uint
	for coin_idx := uint64(1); coin_idx <= numUTXOs; coin_idx++ {
		_, _ = utxof.Discard(32) // readIntoSlice(utxof, prevoutHash[:]) // read 32 bytes
		_, _ = utxof.Discard(4)  // read(utxof, &prevoutIndex)   // read Uint32
		_ = readUvarint(utxof)   // read code VARINT
		_ = readUvarint(utxof)   // read amount VARINT

		if coin_idx%(1024*1024) == 0 {
			log.Printf("%d coins read [%.2f%%], passed since start\n", coin_idx, (float64(coin_idx)/float64(numUTXOs))*100)
		}

		partialScriptPubKey, txType, err := readCompressedScript(utxof)
		if err != nil && !errors.Is(err, errUidentified) {
			log.Println("Error decompressing script:", err.Error())
		}
		switch txType {
		case P2PK, P2PKU:
			pubKeys[[PubKeySize]byte(partialScriptPubKey)] = txType
		case P2PKH, P2SH, P2WPKH:
			hashes[[HashSize]byte(partialScriptPubKey)] = txType
		case P2WSH:
			witnessProgs[[WitnessProgSize]byte(partialScriptPubKey)] = txType
		default:
			unidentifiedScripts++
		}
	}

	log.Printf("Hashes (P2PKH, P2SH, P2WPKH): %d (%.2f%%)\n", len(hashes), float64(len(hashes))/float64(numUTXOs)*100)
	log.Printf("PubKeys (P2PK): %d (%.2f%%)\n", len(pubKeys), float64(len(pubKeys))/float64(numUTXOs)*100)
	log.Printf("WitnessProgs (P2WSH): %d (%.2f%%)\n", len(witnessProgs), float64(len(witnessProgs))/float64(numUTXOs)*100)
	log.Printf("Unidentified scripts: %d (%.2f%%)\n", unidentifiedScripts, float64(unidentifiedScripts)/float64(numUTXOs)*100)
}

func CheckHash160(hash [HashSize]byte) (TxType, bool) {
	txType, ok := hashes[hash]
	return txType, ok
}

func CheckPubKey(pubKey [PubKeySize]byte) (TxType, bool) {
	txType, ok := pubKeys[pubKey]
	return txType, ok
}

func CheckWitnessProg(witnessProg [WitnessProgSize]byte) (TxType, bool) {
	txType, ok := witnessProgs[witnessProg]
	return txType, ok
}
