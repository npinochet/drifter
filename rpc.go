package main

import (
	"encoding/json"
	"log"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/rpcclient"
)

type hasTxsCmd struct{ Address string }

var connCfg = &rpcclient.ConnConfig{
	Host:         "localhost:8334",
	User:         "admin",
	Pass:         "admin",
	HTTPPostMode: true,
	DisableTLS:   true,
}
var client *rpcclient.Client

func init() {
	btcjson.MustRegisterCmd("hastxs", (*hasTxsCmd)(nil), 0)

	var err error
	client, err = rpcclient.New(connCfg, nil)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping()
	if err != nil {
		log.Fatal(err)
	}
}

func rpcHasTxs(address string) (bool, error) {
	response := client.SendCmd(&hasTxsCmd{Address: address})
	res, err := rpcclient.ReceiveFuture(response)
	if err != nil {
		return false, err
	}

	var hasTxs bool
	if err = json.Unmarshal(res, &hasTxs); err != nil {
		return false, err
	}

	return hasTxs, nil
}
