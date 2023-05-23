package main

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/npinochet/drifter/utxo"
)

var (
	checkTime                  = 1 * time.Hour
	lookupTableIndexSize int64 = 24
	checked              atomic.Uint64
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Input a valid bitcoin core RPC 'dumptxoutset' utxo filepath, and a filepath to store the lookup table")
		os.Exit(1)
	}
	utxo.ReadUTXOFile(os.Args[1], os.Args[2], lookupTableIndexSize)

	numCPU := runtime.NumCPU()
	for i := 0; i < numCPU; i++ {
		go worker()
	}
	log.Printf("spawned %d workers\n", numCPU)
	now := time.Now()
	for {
		prevElapsed := time.Since(now)
		prevKeys := checked.Load()
		time.Sleep(checkTime)
		elapsed := time.Since(now)
		keys := checked.Load()
		speed := float64(keys-prevKeys) / (elapsed - prevElapsed).Seconds()
		log.Printf("%d keys checked [%.2f k/s], biggest collision depth yet: %d\n", keys, speed, utxo.GetBiggestCollisionDepth())
	}
}

func worker() {
	for {
		if err := checkRandomKey(); err != nil {
			log.Printf("worker err: %s\n", err)
		}
		checked.Add(1)
	}
}

func checkRandomKey() error {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return fmt.Errorf("could not generate private key: %w", err)
	}
	// check P2PK, P2PKU
	pubKey := privKey.PubKey().SerializeCompressed()
	if txType, exists := utxo.CheckDataExistance(pubKey, utxo.PubKeyType); exists {
		return jackpot(privKey, txType)
	}

	// check P2PKH, P2SH, P2WPKH
	hash := btcutil.Hash160(pubKey)
	if txType, exists := utxo.CheckDataExistance(hash, utxo.HashType); exists {
		return jackpot(privKey, txType)
	}

	// check P2WSH
	script := make([]byte, 35)
	script[0] = 0x21
	copy(script[1:34], pubKey)
	script[34] = 0xac

	witnessProg := sha256.Sum256(script)
	if txType, exists := utxo.CheckDataExistance(witnessProg[:], utxo.WitnessProgType); exists {
		return jackpot(privKey, txType)
	}

	// check P2SH-P2WPKH
	scriptP2WPKH := make([]byte, 22)
	scriptP2WPKH[0] = 0x00
	scriptP2WPKH[1] = 0x14
	copy(scriptP2WPKH[2:22], hash)
	scriptHash := btcutil.Hash160(scriptP2WPKH)
	if txType, exists := utxo.CheckDataExistance(scriptHash, utxo.HashType); exists {
		return jackpot(privKey, txType)
	}

	return nil
}

func jackpot(privKey *btcec.PrivateKey, txType utxo.TxType) error {
	payload := base58.Encode(privKey.Serialize()) + ":" + txType.String()
	log.Println("JACKPOT", payload)

	var fileErr error
	f, err := os.OpenFile("jackpot.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		defer f.Close()
		if _, err := f.WriteString(payload + "\n"); err != nil {
			fileErr = fmt.Errorf("could not write to file: %w", err)
		}
	}
	if err != nil {
		fileErr = fmt.Errorf("could not open file: %w", err)
	}

	var sendErr error
	if err := sendTelegramMessage("JACKPOT " + payload); err != nil {
		sendErr = fmt.Errorf("could not send telegram message: %w", err)
	}

	return errors.Join(fileErr, sendErr)
}

func sendTelegramMessage(message string) error {
	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	params := url.Values{}
	params.Add("chat_id", os.Getenv("TELEGRAM_TARGET_CHAT_ID"))
	params.Add("text", message)
	params.Add("parse_mode", "HTML")
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage?%s", token, params.Encode())
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		content, _ := io.ReadAll(resp.Body)

		return fmt.Errorf("invalid response (%d): %s", resp.StatusCode, content)
	}

	return err
}
