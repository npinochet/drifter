package main

import (
	"bufio"
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
	"github.com/btcsuite/btcd/chaincfg"
)

var (
	checkTime = 1 * time.Hour
	checked   atomic.Uint64
)

func test() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		addr, err := btcutil.DecodeAddress(text[:len(text)-1], &chaincfg.MainNetParams)
		if err != nil {
			fmt.Println(err)
			continue
		}

		hash := addr.(*btcutil.AddressPubKeyHash).Hash160()
		//addr, _ := btcutil.DecodeAddress("1PseiG5Lk9vpUiVEuMh28gqpt2npcvnykJ", &chaincfg.MainNetParams) // 1FWQiwK27EnGXb6BiBMRLJvunJQZZPMcGd
		fmt.Println(fastHasTxs(hash[:]))
	}
}

func main() {
	test()
	return

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
	pubKey := privKey.PubKey()
	pubKeyHash1, pubKeyHash2 := btcutil.Hash160(pubKey.SerializeCompressed()), btcutil.Hash160(pubKey.SerializeUncompressed())
	exists1, err1 := hasTxs(pubKeyHash1)
	exists2, err2 := hasTxs(pubKeyHash2)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("could not query hastxs address: %w, %w", err1, err2)
	}
	if exists1 || exists2 {
		addr1, _ := btcutil.NewAddressPubKeyHash(pubKeyHash1, &chaincfg.MainNetParams)
		return jackpot(privKey, addr1)
	}

	return nil
}

func jackpot(privKey *btcec.PrivateKey, address *btcutil.AddressPubKeyHash) error {
	payload := base58.Encode(privKey.Serialize())
	if address != nil {
		payload = address.String() + ":" + payload
	}
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
