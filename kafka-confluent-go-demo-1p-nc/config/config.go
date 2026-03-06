package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// KafkaConfig holds Kafka connection and topic configuration.
type KafkaConfig struct {
	Topic              string `json:"topic"`
	Topic2             string `json:"topic2"`
	GroupId            string `json:"group.id"`
	GroupId2           string `json:"group.id2,omitempty"`  // 第二个消费者组的 ID（可选）
	BootstrapServers   string `json:"bootstrap.servers"`
	SecurityProtocol   string `json:"security.protocol"`
	SslCaLocation      string `json:"ssl.ca.location,omitempty"`
	SaslMechanism      string `json:"sasl.mechanism"`
	SaslUsername       string `json:"sasl.username"`
	SaslPassword       string `json:"sasl.password"`
}

// DefaultSslCaLocation is used when SecurityProtocol is SASL_SSL and SslCaLocation is empty.
const DefaultSslCaLocation = "./conf/mix-4096-ca-cert"

// LoadFromFile loads KafkaConfig from a JSON file.
// If path is empty, uses "conf/kafka.json" relative to current working directory.
func LoadFromFile(path string) (*KafkaConfig, error) {
	if path == "" {
		workPath, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("get working directory: %w", err)
		}
		path = filepath.Join(workPath, "conf", "kafka.json")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer file.Close()

	var cfg KafkaConfig
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config %s: %w", path, err)
	}

	if cfg.SecurityProtocol == "SASL_SSL" && cfg.SslCaLocation == "" {
		cfg.SslCaLocation = DefaultSslCaLocation
	}

	return &cfg, nil
}

// MustLoad loads config from file or panics. Convenience for main packages.
func MustLoad(path string) *KafkaConfig {
	cfg, err := LoadFromFile(path)
	if err != nil {
		panic(err)
	}
	return cfg
}
