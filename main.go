package main

import (
	"fmt"
	"log"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg"
)

func main() {
	privKey, err := btcec.NewPrivateKey()
	if err != nil {
		log.Panicf("could not generate private key: %s\n", err)
	}
	fmt.Println(privKey)
	pubKeyHash := btcutil.Hash160(privKey.PubKey().SerializeCompressed())
	addr, err := btcutil.NewAddressPubKeyHash(pubKeyHash, &chaincfg.MainNetParams)
	if err != nil {
		log.Panicf("could not generate address: %s\n", err)
	}
	address := addr.String()
	fmt.Println(address)
	has, err := hasTxs(address)
	if err != nil {
		log.Panicf("could not query hastxs address: %s\n", err)
	}
	log.Println(has)
}
