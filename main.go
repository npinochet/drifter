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
	checkTime = 1 * time.Hour
	checked   atomic.Uint64
)

// How to check addresses
// generate pubKey -> check P2PK, P2PKU (33 bytes)
// witnessProg = sha256(0x21 + pubKey + 0xac) -> check P2WSH (32 bytes)
// hash = hash160(pubKey) -> check P2PKH, P2SH, P2WPKH (20 bytes)
// script_hash = hash160(0x0014 + hash) -> check P2SH(-P2WPKH) (20 bytes)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Must input a valid bitcoin RPC 'dumptxoutset' utxo file")
		os.Exit(1)
	}
	utxo.ReadUTXOFile(os.Args[1])

	numCPU := runtime.NumCPU()
	for i := 0; i < numCPU; i++ {
		go worker()
	}
	log.Printf("spawned %d workers\n", numCPU)
	now := time.Now()
	for {
		time.Sleep(checkTime)
		elapsed := time.Since(now)
		addresses := checked.Load()
		speed := float64(addresses) / elapsed.Seconds()
		log.Printf("%d keys checked (%.2f k/s)\n", addresses, speed)
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
	if txType, exists := utxo.CheckPubKey([utxo.PubKeySize]byte(pubKey)); exists {
		return jackpot(privKey, txType)
	}

	// check P2PKH, P2SH, P2WPKH
	hash := btcutil.Hash160(pubKey)
	if txType, exists := utxo.CheckHash160([utxo.HashSize]byte(hash)); exists {
		return jackpot(privKey, txType)
	}

	// check P2WSH
	var script [35]byte
	script[0] = 0x21
	copy(script[1:34], pubKey)
	script[34] = 0xac

	hasher := sha256.New()
	_, _ = hasher.Write(script[:])
	witnessProg := hasher.Sum(nil)
	if txType, exists := utxo.CheckWitnessProg([utxo.WitnessProgSize]byte(witnessProg)); exists {
		return jackpot(privKey, txType)
	}

	// check P2SH-P2WPKH
	scriptP2WPKH := [22]byte{0x00, 0x14}
	copy(scriptP2WPKH[1:22], hash)
	scriptHash := btcutil.Hash160(scriptP2WPKH[:])
	if txType, exists := utxo.CheckHash160([utxo.HashSize]byte(scriptHash)); exists {
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
