package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkagodemo/config"
)

func doInitConsumer(cfg *config.KafkaConfig) *kafka.Consumer {
	fmt.Print("init kafka consumer, it may take a few seconds to init the connection\n")
	//common arguments
	var kafkaconf = &kafka.ConfigMap{
		"api.version.request": "true",
		"auto.offset.reset": "latest",
		"heartbeat.interval.ms": 3000,
		"session.timeout.ms": 30000,
		"max.poll.interval.ms": 120000,
		"fetch.max.bytes": 1024000,
		"max.partition.fetch.bytes": 256000}
	kafkaconf.SetKey("bootstrap.servers", cfg.BootstrapServers);
	kafkaconf.SetKey("group.id", cfg.GroupId)

	switch cfg.SecurityProtocol {
	case "PLAINTEXT" :
		kafkaconf.SetKey("security.protocol", "plaintext");
	case "SASL_SSL":
		kafkaconf.SetKey("security.protocol", "sasl_ssl")
		kafkaconf.SetKey("ssl.ca.location", cfg.SslCaLocation)
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername);
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword);
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism);
        // hostname校验改成空
		kafkaconf.SetKey("ssl.endpoint.identification.algorithm", "None");
		kafkaconf.SetKey("enable.ssl.certificate.verification", "false");
	case "SASL_PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "sasl_plaintext");
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername);
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword);
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism)

	default:
		panic(kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true))
	}

	consumer, err := kafka.NewConsumer(kafkaconf)
	if err != nil {
		panic(err)
	}
	fmt.Print("init kafka consumer success\n")
	return consumer;
}

func main() {

	// Choose the correct protocol
	// 9092 for PLAINTEXT
	// 9093 for SASL_SSL, need to provide sasl.username and sasl.password
	// 9094 for SASL_PLAINTEXT, need to provide sasl.username and sasl.password
	cfg := config.MustLoad("")
	consumer := doInitConsumer(cfg)

	consumer.SubscribeTopics([]string{cfg.Topic, cfg.Topic2}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will
			//automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	consumer.Close()
}