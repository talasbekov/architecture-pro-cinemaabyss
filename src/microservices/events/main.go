package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Config holds service configuration
type Config struct {
	Port         string
	KafkaBrokers []string
}

// Event types
type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type,omitempty"`
}

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

var (
	config   Config
	producer sarama.SyncProducer
)

const (
	TopicMovieEvents   = "movie-events"
	TopicUserEvents    = "user-events"
	TopicPaymentEvents = "payment-events"
)

func main() {
	loadConfig()

	log.Printf("Events Service starting on port %s", config.Port)
	log.Printf("Kafka Brokers: %v", config.KafkaBrokers)

	// Initialize Kafka producer
	var err error
	producer, err = createProducer()
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Start consumers in background
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(3)
	go startConsumer(ctx, &wg, TopicMovieEvents)
	go startConsumer(ctx, &wg, TopicUserEvents)
	go startConsumer(ctx, &wg, TopicPaymentEvents)

	// Set up HTTP routes
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	// Handle graceful shutdown
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		<-sigchan
		log.Println("Shutting down...")
		cancel()
		wg.Wait()
		os.Exit(0)
	}()

	// Start server
	log.Fatal(http.ListenAndServe(":"+config.Port, nil))
}

func loadConfig() {
	config.Port = getEnv("PORT", "8082")

	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	config.KafkaBrokers = strings.Split(brokers, ",")
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func createProducer() (sarama.SyncProducer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Return.Successes = true

	// Retry connection with backoff
	var producer sarama.SyncProducer
	var err error

	for i := 0; i < 10; i++ {
		producer, err = sarama.NewSyncProducer(config.KafkaBrokers, saramaConfig)
		if err == nil {
			log.Println("Successfully connected to Kafka")
			return producer, nil
		}
		log.Printf("Failed to connect to Kafka (attempt %d/10): %v", i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}

	return nil, err
}

func startConsumer(ctx context.Context, wg *sync.WaitGroup, topic string) {
	defer wg.Done()

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	// Retry connection
	var consumer sarama.Consumer
	var err error

	for i := 0; i < 10; i++ {
		consumer, err = sarama.NewConsumer(config.KafkaBrokers, saramaConfig)
		if err == nil {
			break
		}
		log.Printf("[%s] Failed to create consumer (attempt %d/10): %v", topic, i+1, err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}

	if err != nil {
		log.Printf("[%s] Could not create consumer after retries: %v", topic, err)
		return
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Printf("[%s] Failed to start partition consumer: %v", topic, err)
		return
	}
	defer partitionConsumer.Close()

	log.Printf("[CONSUMER] Started consuming from topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[CONSUMER] Stopping consumer for topic: %s", topic)
			return
		case err := <-partitionConsumer.Errors():
			log.Printf("[CONSUMER][%s] Error: %v", topic, err)
		case msg := <-partitionConsumer.Messages():
			log.Printf("[CONSUMER][%s] Received message: partition=%d, offset=%d, value=%s",
				topic, msg.Partition, msg.Offset, string(msg.Value))
			processMessage(topic, msg.Value)
		}
	}
}

func processMessage(topic string, value []byte) {
	var event Event
	if err := json.Unmarshal(value, &event); err != nil {
		log.Printf("[PROCESSOR][%s] Failed to unmarshal event: %v", topic, err)
		return
	}

	log.Printf("[PROCESSOR][%s] Processing event: id=%s, type=%s, timestamp=%s",
		topic, event.ID, event.Type, event.Timestamp)

	// Here you could add more complex processing logic
	switch topic {
	case TopicMovieEvents:
		log.Printf("[PROCESSOR][MOVIE] Event processed successfully: %s", event.ID)
	case TopicUserEvents:
		log.Printf("[PROCESSOR][USER] Event processed successfully: %s", event.ID)
	case TopicPaymentEvents:
		log.Printf("[PROCESSOR][PAYMENT] Event processed successfully: %s", event.ID)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var movieEvent MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&movieEvent); err != nil {
		log.Printf("[API] Failed to decode movie event: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("movie-%d-%s", movieEvent.MovieID, movieEvent.Action),
		Type:      "movie",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   movieEvent,
	}

	partition, offset, err := publishEvent(TopicMovieEvents, event)
	if err != nil {
		log.Printf("[API] Failed to publish movie event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[API] Movie event published: topic=%s, partition=%d, offset=%d",
		TopicMovieEvents, partition, offset)

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var userEvent UserEvent
	if err := json.NewDecoder(r.Body).Decode(&userEvent); err != nil {
		log.Printf("[API] Failed to decode user event: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("user-%d-%s", userEvent.UserID, userEvent.Action),
		Type:      "user",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   userEvent,
	}

	partition, offset, err := publishEvent(TopicUserEvents, event)
	if err != nil {
		log.Printf("[API] Failed to publish user event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[API] User event published: topic=%s, partition=%d, offset=%d",
		TopicUserEvents, partition, offset)

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var paymentEvent PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&paymentEvent); err != nil {
		log.Printf("[API] Failed to decode payment event: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        fmt.Sprintf("payment-%d-%s", paymentEvent.PaymentID, paymentEvent.Status),
		Type:      "payment",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   paymentEvent,
	}

	partition, offset, err := publishEvent(TopicPaymentEvents, event)
	if err != nil {
		log.Printf("[API] Failed to publish payment event: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[API] Payment event published: topic=%s, partition=%d, offset=%d",
		TopicPaymentEvents, partition, offset)

	response := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

func publishEvent(topic string, event Event) (int32, int64, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(event.ID),
		Value: sarama.ByteEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, nil
}
