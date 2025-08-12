package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	BROKER_ADDR = "localhost:9092"
	TOPIC_NAME  = "videos"
)

func main() {
	w := &kafka.Writer{
		Addr:         kafka.TCP(BROKER_ADDR),
		Topic:        TOPIC_NAME,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	log.Println("Publisher ready. Type message and hit Enter. Empty line to exit.")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("> ")
		line, _ := reader.ReadString('\n')
		if len(line) <= 1 {
			break
		}
		key := []byte(fmt.Sprintf("video-%d", rand.IntN(5)))
		msg := kafka.Message{
			Key:   key,
			Value: []byte(line),
			Time:  time.Now(),
		}

		if err := w.WriteMessages(context.Background(), msg); err != nil {
			log.Fatalf("write failed: %v", err)
		}
		log.Printf("sent: key=%s, value=%s", key, line)
	}
}
