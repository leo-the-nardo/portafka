package entity

import (
	"context"
	"fmt"
	"log"
	"net"

	//"github.com/IBM/sarama"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"time"
)

type TcpProcessor struct {
	broker           []string
	port             string
	topic            string
	producer         *kgo.Client
	mode             string
	listener         net.Listener
	handleConnection func(conn net.Conn)
	messageCh        chan Message
	errorCh          chan ErrorProducing
	messageMaxBytes  int32
	maxBufferRecords int
}

func NewTcpProcessor(broker []string, port, topic, mode string, messageCh chan Message, messageMaxBytes int32, maxBufferRecords int) (*TcpProcessor, error) {
	t := &TcpProcessor{
		broker:           broker,
		port:             port,
		topic:            topic,
		mode:             mode,
		messageCh:        messageCh,
		messageMaxBytes:  messageMaxBytes,
		maxBufferRecords: maxBufferRecords,
	}
	//kafkaConfig := sarama.NewConfig()
	var opts []kgo.Opt
	switch mode {
	case "fast":
		t.handleConnection = t.handleConnectionFastMode
		//	config kgo produce options
		opts = []kgo.Opt{
			kgo.SeedBrokers(broker...),
			kgo.DefaultProduceTopic(topic),
			kgo.ClientID(fmt.Sprintf("%s-%s", topic, port)),
			kgo.ProducerBatchCompression(kgo.Lz4Compression()),
			kgo.RequiredAcks(kgo.NoAck()),
			kgo.ProducerBatchMaxBytes(messageMaxBytes),
			kgo.MaxBufferedRecords(maxBufferRecords),
		}
	case "consistent":
		t.handleConnection = t.handleConnectionConsistencyMode
		opts = []kgo.Opt{
			kgo.SeedBrokers(broker...),
			kgo.DefaultProduceTopic(topic),
			kgo.ClientID(fmt.Sprintf("%s-%s", topic, port)),
			kgo.ProducerBatchCompression(kgo.Lz4Compression()),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.ProducerBatchMaxBytes(messageMaxBytes),
			kgo.MaxBufferedRecords(maxBufferRecords),
		}
	default:
		t.handleConnection = t.handleConnectionFastMode
		t.handleConnection = t.handleConnectionConsistencyMode
		opts = []kgo.Opt{
			kgo.SeedBrokers(broker...),
			kgo.DefaultProduceTopic(topic),
			kgo.ClientID(fmt.Sprintf("%s-%s", topic, port)),
			kgo.ProducerBatchCompression(kgo.Lz4Compression()),
			kgo.ProducerBatchMaxBytes(messageMaxBytes),
			kgo.MaxBufferedRecords(maxBufferRecords),
		}

	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		// TODO: handle/log this error
		return nil, err
	}
	defer client.Close()
	t.producer = client

	return t, nil
}

func (t *TcpProcessor) StartListener() {
	port := t.port
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
		go t.handleConnection(conn)
	}
}

func (t *TcpProcessor) handleConnectionFastMode(conn net.Conn) {
	defer conn.Close()                        // On fast mode, close the connection after first read
	buffer := make([]byte, t.messageMaxBytes) // Create a buffer to allocate the content of the message
	n, err := conn.Read(buffer)               // Allocate the content of the message into the buffer
	if err != nil {
		log.Fatal(err)
	}
	data := buffer[:n] // Take the actual content of the message
	go func() {
		t.messageCh <- Message{Topic: t.topic, Content: data} // fire the message to the channel but don't wait for it if the channel is full to close the connection
	}()
}

func (t *TcpProcessor) handleConnectionConsistencyMode(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, t.messageMaxBytes) // Create a buffer to allocate the content of the message
	for {
		n, err := conn.Read(buffer) // Read up to 1024 bytes into the buffer
		if err != nil {
			if err == io.EOF {
				log.Printf("Connection closed by client.")
				return
			}
			log.Printf("Error reading from connection: %v", err)
			return
		}
		data := buffer[:n] // Slice the buffer to the actual number of bytes read
		go func() {
			t.messageCh <- Message{Topic: t.topic, Content: data} // fire the message to the channel but don't wait for it if the channel is full to close the connection
		}()
		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))
		conn.Write([]byte("ok"))
	}
}

// StartProducingToKafka consumes messages from messageCh and sends them to Kafka asynchronously.
// If an error occurs during production, it is sent to errorCh.
func (t *TcpProcessor) StartProducingToKafka(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// If the context is canceled, exit the loop to stop producing
			return
		case message := <-t.messageCh:
			t.producer.Produce(
				ctx,
				&kgo.Record{
					Topic: message.Topic,
					Value: message.Content,
				},
				t.onErrorProducing,
			)
		}
	}
}

func (t *TcpProcessor) onErrorProducing(record *kgo.Record, err error) {
	// log error
	log.Default().Printf("Failed to produce message to topic %s: %v", record.Topic, err)
}
