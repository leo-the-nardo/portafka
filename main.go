package main

import (
	"github.com/IBM/sarama"
	"io"
	"os"
)

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

type Config struct {
	Brokers  []string          `json:"brokers"`
	TCPPorts map[string]string `json:"tcp_ports"`
	UDPPorts map[string]string `json:"udp_ports"`
}

var producer sarama.SyncProducer
var messageChan chan Message

type Message struct {
	Topic   string
	Content []byte
}

func main() {
	// Load configuration
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize Kafka producer
	producer, err = newKafkaProducer(config.Brokers)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}

	// Initialize buffered channel
	messageChan = make(chan Message, 10000)

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start TCP listeners for each port
	for portStr, topic := range config.TCPPorts {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("Invalid TCP port: %v", portStr)
		}
		wg.Add(1)
		go startTCPListener(port, topic, &wg)
	}

	// Start UDP listeners for each port
	for portStr, topic := range config.UDPPorts {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			log.Fatalf("Invalid UDP port: %v", portStr)
		}
		wg.Add(1)
		go startUDPListener(port, topic, &wg)
	}

	// Start a goroutine to process messages from the channel
	wg.Add(1)
	go processMessages(&wg)

	// Wait for all goroutines to finish
	wg.Wait()
}

func loadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	bytes, err := io.ReadAll(file)
	//jsonData := `{
	//	"brokers": ["localhost:9094"],
	//	"tcp_ports": {
	//		"7777": "topic1",
	//		"7778": "topic2"
	//	},
	//	"udp_ports": {
	//		"8888": "topic3",
	//		"8889": "topic4"
	//	}
	//}`
	if err != nil {
		return nil, err
	}
	//bytes := []byte(jsonData)
	var config Config
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func newKafkaProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Flush.MaxMessages = 1000
	config.Producer.Flush.Bytes = 1048576 // 1MB
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	return producer, nil
}

func startTCPListener(port int, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	address := fmt.Sprintf(":%d", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port %d: %v", port, err)
	}
	defer listener.Close()
	log.Printf("Listening on TCP port %d", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection on port %d: %v", port, err)
			continue
		}
		go handleTCPConnection(conn, topic)
	}
}

func handleTCPConnection(conn net.Conn, topic string) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		message := scanner.Bytes()
		messageChan <- Message{Topic: topic, Content: message}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading from connection: %v", err)
	}
}

func startUDPListener(port int, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	address := fmt.Sprintf(":%d", port)
	conn, err := net.ListenPacket("udp", address)
	if err != nil {
		log.Fatalf("Failed to listen on UDP port %d: %v", port, err)
	}
	defer conn.Close()
	log.Printf("Listening on UDP port %d", port)

	buffer := make([]byte, 1024)
	for {
		n, _, err := conn.ReadFrom(buffer)
		if err != nil {
			log.Printf("Error reading from UDP port %d: %v", port, err)
			continue
		}
		message := buffer[:n]
		messageChan <- Message{Topic: topic, Content: message}
	}
}

func processMessages(wg *sync.WaitGroup) {
	defer wg.Done()
	for msg := range messageChan {
		if err := sendToKafka(msg.Topic, msg.Content); err != nil {
			log.Printf("Failed to send message to Kafka: %v", err)
			return
		}
		log.Default().Printf("Message sent to Kafka topic %s", msg.Topic)
	}

}

func sendToKafka(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}
	_, _, err := producer.SendMessage(msg)
	return err
}
