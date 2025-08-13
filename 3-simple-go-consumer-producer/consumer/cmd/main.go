package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	BROKER_ADDR = "localhost:9092"
	TOPIC_NAME  = "video-uploaded"
	GROUP_ID    = "video-processors"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("shutting down")
		cancel()
	}()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{BROKER_ADDR},
		Topic:             TOPIC_NAME,
		GroupID:           GROUP_ID,
		StartOffset:       kafka.LastOffset,
		MinBytes:          1,
		MaxBytes:          10e6,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    12 * time.Second,
		CommitInterval:    0,
	})
	defer r.Close()

	log.Printf("consumer started (group=%s, topic=%s)\n", GROUP_ID, TOPIC_NAME)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("fetch error: %v", err)
			continue
		}
		log.Printf("got: partition=%d offset=%d key=%s value=%q", m.Partition, m.Offset, string(m.Key), string(m.Value))
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}
