package main

import (
	"golang-poc/internal/entity"
	"golang-poc/internal/init"
	"golang-poc/pkg/env"
	"log"
)

func main() {
	// Load configuration
	defaultEnvs := map[string]string{
		"PORTAFKA_CONFIG": "/etc/portafka/config.yaml",
	}
	env.Load(defaultEnvs)
	cfg, err := init.LoadConfig("PORTAFKA_CONFIG")
	if err != nil {
		log.Fatalf("Failed to load init: %v", err)
	}
	// start tcp servers for each tcp given
	var tcpProcessors []*entity.TcpProcessor
	for _, processor := range cfg.Processors.TCP {
		messageCh := make(chan entity.Message, processor.MessagesPerBatch)
		tcpProcessor, err := entity.NewTcpProcessor(
			cfg.Brokers,
			processor.Port,
			processor.Topic,
			processor.Mode,
			messageCh,
			processor.MessageBytesSize,
			processor.MessagesPerBatch,
		)
		if err != nil {
			log.Fatalf("Failed to create TCP entity: %v", err)
		}
		tcpProcessors = append(tcpProcessors, tcpProcessor)
	}

}
