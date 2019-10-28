package kafkalib

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	kafka "github.com/segmentio/kafka-go"
)

type InventoryRecord struct {
	Comment    string `json:"_comment"`
	ItemNumber string `json:"item_number"`
	ActualQty  int    `json:"actual_qty"`
	WhId       string `json:"wh_id"`
	LocationId string `json:"location_id"`
}

// Produce takes a json array of inventory in bytes and produces each
// inventory item on a topic.
func ProduceInventory(inventoryJsonBytes []byte, topicName string) (err error) {
	fmt.Println("Producer started.")

	// make a writer that produces to topic-A, using the least-bytes distribution
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	})

	var inventoryRecords []InventoryRecord
	err = json.Unmarshal(inventoryJsonBytes, &inventoryRecords)
	if err != nil {
		return err
	}

	kafkaMessages := make([]kafka.Message, len(inventoryRecords))
	for i, inventoryRecord := range inventoryRecords {
		inventoryRecordBytes, _ := json.Marshal(inventoryRecord)
		kafkaMessages[i] = kafka.Message{
			Value: inventoryRecordBytes,
		}
	}

	err = w.WriteMessages(context.Background(), kafkaMessages...)
	if err != nil {
		return err
	}

	w.Close()
	fmt.Println("Producer closed.")

	return nil
}

// Consume will listen indefinitely to a kafka topic and print the results.
func Consume(topicName string, groupID string) {
	fmt.Println("Consumer started.")

	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  groupID,
		Topic:    topicName,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
	fmt.Println("Consumer closed.")
}

// Transformer is an interface to a transform operation that can be passed
// in as a custom
type Transformer interface {
	Run(msg kafka.Message) kafka.Message
}

func ConsumeTransformer(topicName string, groupID string, transformer Transformer) {
	fmt.Println("Consumer started.")

	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  groupID,
		Topic:    topicName,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		tm := transformer.Run(m)
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(tm.Key), string(tm.Value))
	}

	r.Close()
	fmt.Println("Consumer closed.")
}

type TxInventoryRecord struct {
	Comment          string `json:"_comment"`
	ItemNumber       string `json:"item_number"`
	ActualQty        int    `json:"actual_qty"`
	ActualQtyUpdated int    `json:"actual_qty_updated"`
	WhId             string `json:"wh_id"`
	LocationId       string `json:"location_id"`
}

func ConsumeTransformerProducer(topicName, groupID, outTopicName string, transformer Transformer) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	fmt.Println("Consumer started.")

	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  groupID,
		Topic:    topicName,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	ch := make(chan kafka.Message)

	// async function to read messages from topic and write to channel
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			tm := transformer.Run(m)
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(tm.Key), string(tm.Value))
			ch <- tm
		}
	}()

	// async function to write messages to out topic from channel
	go func() {
		for tm := range ch {
			// produce recieved messages
			var newInventoryRecord TxInventoryRecord
			err := json.Unmarshal(tm.Value, &newInventoryRecord)
			if err != nil {
				log.Fatal("could not unmarshal from channel", err)
			}
			inventoryRecordBytes, _ := json.Marshal([]InventoryRecord{
				InventoryRecord{
					Comment:    newInventoryRecord.Comment,
					ItemNumber: newInventoryRecord.ItemNumber,
					ActualQty:  newInventoryRecord.ActualQtyUpdated,
					WhId:       newInventoryRecord.WhId,
					LocationId: newInventoryRecord.LocationId,
				},
			})
			err = ProduceInventory(inventoryRecordBytes, outTopicName)
			if err != nil {
				log.Fatal("could not produce messages!", err)
			}
		}
	}()

	// wait until interrupt signal
	<-c

	r.Close()
	fmt.Println("Consumer-Producer closed.")
}
