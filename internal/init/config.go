package init

import (
	"encoding/json"
	"errors"
	"golang-poc/pkg/env"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

// TcpProcessor represents a TCP listener configuration
type TcpProcessor struct {
	Port             string `json:"port" yaml:"port"`
	Topic            string `json:"topic" yaml:"topic"`
	Mode             string `json:"mode" yaml:"mode"`
	MessageBytesSize int32  `json:"messageBytesSize" yaml:"messageBytesSize"`
	MessagesPerBatch int    `json:"messagesPerBatch" yaml:"messagesPerBatch"`
}

// UdpProcessor represents a UDP listener configuration
type UdpProcessor struct {
	Port  string `json:"port" yaml:"port"`
	Topic string `json:"topic" yaml:"topic"`
	Mode  string `json:"mode" yaml:"mode"`
}

// Config represents the application configuration
type Config struct {
	Brokers    []string `json:"brokers" yaml:"brokers"`
	Processors struct {
		TCP []TcpProcessor `json:"tcp" yaml:"tcp"`
		UDP []UdpProcessor `json:"udp" yaml:"udp"`
	} `json:"processors" yaml:"processors"`
}

// LoadConfig loads the configuration from an environment variable or a file.
func LoadConfig(envName string) (*Config, error) {
	configEnv := env.Getenv(envName)
	if configEnv == "" {
		return nil, errors.New("init variable not set")
	}
	// Check if the environment variable is a valid file path
	if isFile(configEnv) {
		return loadConfigFromFile(configEnv)
	}
	// If not a file path, assume it's raw JSON or YAML content
	return loadConfigFromContent(configEnv)
}

// isFile checks if the given path is a valid file
func isFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// loadConfigFromFile loads the init from a file (JSON or YAML)
func loadConfigFromFile(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return loadConfigFromContent(string(bytes))
}

// loadConfigFromContent loads the init from raw JSON or YAML content
func loadConfigFromContent(content string) (*Config, error) {
	var config Config

	// Try to unmarshal as JSON first
	if json.Unmarshal([]byte(content), &config) == nil {
		return &config, nil
	}

	// If JSON unmarshalling fails, try YAML
	if yaml.Unmarshal([]byte(content), &config) == nil {
		return &config, nil
	}

	return nil, errors.New("failed to parse configuration as JSON or YAML")
}
