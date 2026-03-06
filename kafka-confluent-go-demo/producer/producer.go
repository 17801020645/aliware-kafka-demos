package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkagodemo/config"
)

const (
	INT32_MAX = 2147483647 - 1000
)

func doInitProducer(cfg *config.KafkaConfig) *kafka.Producer {
	fmt.Print("init kafka producer, it may take a few seconds to init the connection\n")
	//common arguments
	var kafkaconf = &kafka.ConfigMap{
		"api.version.request": "true",
		"message.max.bytes": 1000000,
		"linger.ms": 500,
		"sticky.partitioning.linger.ms" : 1000,
		"retries": INT32_MAX,
		"retry.backoff.ms": 1000,
		"acks": "1"}
	kafkaconf.SetKey("bootstrap.servers", cfg.BootstrapServers)

	switch cfg.SecurityProtocol {
	case "PLAINTEXT" :
		kafkaconf.SetKey("security.protocol", "plaintext");
	case "SASL_SSL":
		kafkaconf.SetKey("security.protocol", "sasl_ssl")
		kafkaconf.SetKey("ssl.ca.location", cfg.SslCaLocation)
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername)
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword)
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism)
		kafkaconf.SetKey("enable.ssl.certificate.verification", "false")
		kafkaconf.SetKey("ssl.endpoint.identification.algorithm", "None")
	case "SASL_PLAINTEXT":
		kafkaconf.SetKey("security.protocol", "sasl_plaintext");
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername);
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword);
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism)
	default:
		panic(kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true))
	}

	producer, err := kafka.NewProducer(kafkaconf)
	if err != nil {
		panic(err)
	}
	fmt.Print("init kafka producer success\n")
	return producer
}

func main() { // 程序入口函数，启动 Kafka 生产者示例
	// Choose the correct protocol                        // 说明不同端口对应的安全协议
	// 9092 for PLAINTEXT                                 // 明文传输使用的端口
	// 9093 for SASL_SSL, need to provide sasl.username and sasl.password // 启用 SASL_SSL 时使用的端口及鉴权说明
	// 9094 for SASL_PLAINTEXT, need to provide sasl.username and sasl.password // 启用 SASL_PLAINTEXT 时使用的端口及鉴权说明
	cfg := config.MustLoad("") // 从默认路径加载 Kafka 配置（conf/kafka.json）
	producer := doInitProducer(cfg) // 基于配置初始化 Kafka Producer 客户端

	defer producer.Close() // 在 main 结束前关闭 Producer，确保资源被释放

	// Delivery report handler for produced messages // 处理消息投递结果的回调处理逻辑
	go func() { // 启动一个 goroutine 异步消费 Producer 事件
		for e := range producer.Events() { // 不断从事件通道中读取事件
			switch ev := e.(type) { // 根据事件类型做类型断言
			case *kafka.Message: // 只关心消息投递结果事件
				if ev.TopicPartition.Error != nil { // 如果分区上有错误，表示消息发送失败
					log.Printf("Failed to write access log entry:%v", ev.TopicPartition.Error) // 打印发送失败的错误日志
				} else { // 没有错误表示发送成功
					log.Printf("Send OK topic:%v partition:%v offset:%v content:%s\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, ev.Value) // 打印成功发送的 topic、分区、offset 及消息内容

				}
			}
		}
	}() // 立即启动 goroutine 执行上述匿名函数

	// Produce messages to topic (asynchronously) // 异步地向 Kafka 主题写入消息
	i := 0 // 消息计数器，从 0 开始
	for { // 无限循环，持续发送消息
		i = i + 1 // 递增计数器，用于区分每条消息
		value := "this is a kafka message from confluent go " + strconv.Itoa(i) // 构造要发送的消息内容
		var msg *kafka.Message = nil // 定义要发送的 Kafka 消息指针
		if i%2 == 0 { // 偶数序号发送到第二个主题
			msg = &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &cfg.Topic2, Partition: kafka.PartitionAny}, // 目标为 Topic2，分区由 Kafka 自动分配
				Value:          []byte(value),                                                          // 消息体为构造的字符串
			}
		} else { // 奇数序号发送到第一个主题
			msg = &kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &cfg.Topic, Partition: kafka.PartitionAny}, // 目标为 Topic，分区由 Kafka 自动分配
				Value:          []byte(value),                                                         // 消息体为构造的字符串
			}
		}
		producer.Produce(msg, nil) // 将消息异步投递到 Kafka 集群
		time.Sleep(time.Duration(1) * time.Millisecond) // 每次发送后短暂休眠，避免过快发送
		break
	}
	// Wait for message deliveries before shutting down // 理论上等待所有消息发送完成再退出（死循环下不会执行到这里）
	producer.Flush(15 * 1000) // 等待最长 15 秒以刷新缓冲区中的消息
}
