package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/btcutil/base58"
	"github.com/btcsuite/btcd/chaincfg"
)

var (
	checkTime = 1 * time.Hour
	checked   atomic.Uint64
)

func main() {
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
		log.Printf("%d addresses checked (%.2f A/s)\n", addresses, speed)
	}
}

func worker() {
	for {
		if err := checkRandomAddress(); err != nil {
			log.Printf("worker err: %s\n", err)
		}
		checked.Add(1)
	}
}

func checkRandomAddress() error {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		return fmt.Errorf("could not generate private key: %w", err)
	}
	pubKeyHash := btcutil.Hash160(privKey.PubKey().SerializeCompressed())
	addr, err := btcutil.NewAddressPubKeyHash(pubKeyHash, &chaincfg.MainNetParams)
	if err != nil {
		return fmt.Errorf("could not generate address: %w", err)
	}
	address := addr.String()
	exists, err := rpcHasTxs(address)
	if err != nil {
		return fmt.Errorf("could not query hastxs address: %w", err)
	}
	if !exists && rand.Float64() < 0.1 {
		return jackpot(privKey, address)
	}

	return nil
}

func jackpot(privKey *btcec.PrivateKey, address string) error {
	payload := address + ":" + base58.Encode(privKey.Serialize())
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
