package main

import (
	"io/ioutil"
	"log"

	"github.com/XiangruL/kafkaConsumerTrigger/kafkalib"
)

func main() {
	var err error

	jsonBytes, err := ioutil.ReadFile("data/inventory.json")
	if err != nil {
		log.Fatal(err)
	}

	err = kafkalib.ProduceInventory(jsonBytes, "libtest")
	if err != nil {
		log.Fatal(err)
	}
}
