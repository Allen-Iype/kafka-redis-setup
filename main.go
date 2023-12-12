package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	redis "github.com/go-redis/redis/v8"
	"github.com/gorilla/mux"
)

var kafkaProducer sarama.SyncProducer

type Commit struct {
	Id         int    `json:"id"`
	Name       string `json:"name"`
	Repository string `json:"repository"`
	Message    string `json:"message"`
}

func init() {
	// Initializing the Kafka Producer
	log.Println("Initializing Kafka Producer")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	kafkaProducer = producer
}

func publishToKafka(producer sarama.SyncProducer, data []byte, topic string) error {
	// Creating a new message to send to the Kafka topic
	log.Println("Topic to which message is publish : ", topic)
	log.Println("Message send to Kafka Topic : ", string(data))
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}

	// Sending the message to the Kafka topic
	partition, Offset, err := producer.SendMessage(message)
	log.Println("Partition", partition)
	log.Println("Offset", Offset)
	if err != nil {
		return err
	}

	return nil
}

func PostHandler(w http.ResponseWriter, r *http.Request) {
	// Parse the data from the request body
	var req Commit
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Error parsing request body", http.StatusBadRequest)
		return
	}
	log.Println(req)
	data, err1 := json.Marshal(req)
	if err1 != nil {
		log.Println("Error marshalling req")
		return
	}

	// Use a Goroutine to send data to Kafka
	go func() {
		// Publish the data to the Kafka topic
		err := publishToKafka(kafkaProducer, data, "commits")
		if err != nil {
			// Handle Kafka publish error
			log.Println("Error publishing to Kafka:", err)
		}
	}()

	// Respond to the client
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Request received and being processed"))
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from the request, e.g., a key for Redis
	key := r.URL.Query().Get("key")

	// Create a channel to communicate the result to the main function
	resultCh := make(chan string)
	defer close(resultCh) // Close the channel when done

	// Use a Goroutine to perform background tasks
	go func() {
		// Create a Redis client
		redisClient := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
			Password: "", // No password by default
			DB:       0,  // Default database
		})

		// Retrieve data from Redis using the provided key
		data, err := redisClient.Get(context.Background(), key).Result()
		if err != nil {
			log.Printf("Error retrieving data from Redis: %v", err)
			resultCh <- "" // Send an empty string on the channel to indicate an error
			return
		}

		// Encode the data as JSON
		dataJSON, err := json.Marshal(data)
		if err != nil {
			log.Printf("Error encoding data as JSON: %v", err)
			resultCh <- "" // Send an empty string on the channel to indicate an error
			return
		}

		// Publish the data to a new Kafka topic
		err = publishToKafka(kafkaProducer, dataJSON, "commits-2")
		if err != nil {
			log.Printf("Error publishing to Kafka: %v", err)
		}

		// Send the data on the channel
		resultCh <- string(dataJSON)
	}()

	// Read the result from the channel
	result := <-resultCh

	if result == "" {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error processing data"))
	} else {
		// Set the content type to JSON and respond with the encoded data
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(result))
	}
}

// Create a unique key for a Commit
func (c *Commit) RedisKey() string {
	// Use a prefix that makes sense for your application
	prefix := "commit"

	// Combine the prefix and a unique identifier (e.g., the commit ID)
	return fmt.Sprintf("%s:%d", prefix, c.Id)
}

func KafkaConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer: %v", err)
		return
	}
	defer consumer.Close()

	topic := "commits"
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Error creating Kafka partition consumer: %v", err)
		return
	}
	defer partitionConsumer.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password by default
		DB:       0,  // Default database
	})
	defer redisClient.Close()

	for msg := range partitionConsumer.Messages() {
		// Process the Kafka message
		log.Println("[Consumer go routine] The subsrcibed topic : ", msg.Topic)
		data := msg.Value
		var commit Commit
		err := json.Unmarshal(data, &commit)
		if err != nil {
			// Handle the unmarshaling error
			log.Printf("Error unmarshaling JSON: %v\n", err)
			return
		}
		key := commit.RedisKey()
		log.Println("[Consumer go routine] The message received : ", commit)
		// Store the data in Redis
		log.Println("[Consumer go routine] Redis key : ", key)
		err1 := redisClient.Set(context.Background(), key, data, 0).Err()
		if err1 != nil {
			log.Printf("Error storing data in Redis: %v", err1)
		}
	}
}

func Bootup() {
	log.Println("Server Started")
	r := mux.NewRouter()

	r.HandleFunc("/api/v1/push-data", PostHandler).Methods("POST")
	r.HandleFunc("/api/v1/get-data", GetHandler).Methods("GET")
	go KafkaConsumer()

	err := http.ListenAndServe(":8080", r)
	if err != nil {
		log.Printf("Error starting server: %s\n", err)
	}
}
func main() {
	Bootup()
	// go KafkaConsumer()
}
