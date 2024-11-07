package entity

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"net"
)

type UdpProcessor struct {
	broker          string
	port            string
	topic           string
	producer        sarama.SyncProducer
	mode            string
	listener        net.Listener
	channelHolder   *ChannelHolder
	messageMaxBytes int
}

func NewUdpProcessor(broker, port, topic string, producer sarama.SyncProducer, channelHolder *ChannelHolder, messageMaxBytes int) *UdpProcessor {
	u := &UdpProcessor{
		broker:          broker,
		port:            port,
		topic:           topic,
		producer:        producer,
		mode:            "fastMode",
		channelHolder:   channelHolder,
		messageMaxBytes: messageMaxBytes,
	}
	return u
}

func (u *UdpProcessor) startListener() {
	port := u.port
	//address := fmt.Sprintf(":%d", port)
	address, err := net.ResolveUDPAddr("udp", ":8080")
	if err != nil {
		fmt.Println("Error resolving address:", err)
		return
	}
	listener, err := net.ListenUDP("udp", address)
	if err != nil {
		log.Fatalf("Failed to listen on TCP port %d: %v", port, err)
	}
	defer listener.Close()
	log.Printf("Listening on TCP port %d", port)
	for {
		// Read from the connection
		buffer := make([]byte, u.messageMaxBytes)
		n, _, err := listener.ReadFromUDP(buffer)
		if err != nil {
			fmt.Println("Error reading:", err)
			continue
		}
		data := buffer[:n]
		u.channelHolder.messageCh <- Message{Topic: u.topic, Content: data} // fire the message to the channel but don'u wait for it if the channel is full to close the connection
		//go func() { //todo: avaliate if this goroutine is necessary
		//	u.channelHolder.messageCh <- Message{Topic: u.topic, Content: data} // fire the message to the channel but don'u wait for it if the channel is full to close the connection
		//}()
	}
}

//
//func (u *UdpProcessor) handleConnection(conn net.Conn) {
//	defer conn.Close()                        // On fast mode, close the connection after first read
//	buffer := make([]byte, u.messageMaxBytes) // Create a buffer to allocate the content of the message
//	n, err := conn.Read(buffer)               // Allocate the content of the message into the buffer
//	if err != nil {
//		log.Fatal(err)
//	}
//	data := buffer[:n] // Take the actual content of the message
//	go func() {        //todo: avaliate if this goroutine is necessary
//		u.channelHolder.messageCh <- Message{Topic: u.topic, Content: data} // fire the message to the channel but don'u wait for it if the channel is full to close the connection
//	}()
//}
