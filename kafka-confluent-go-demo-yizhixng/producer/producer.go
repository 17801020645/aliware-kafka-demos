// 顺序性 Producer 示例
//
// 本示例演示 Kafka 消息顺序性保证：通过 Key 精准路由 + 幂等性 + 限制在途请求，
// 确保有顺序关系的消息正确落入同一分区，且重试不会打乱顺序。
//
// 配置说明（均在 producer 代码内，未修改 conf/kafka.json）：
// - acks=all：幂等性要求；若集群 min.insync.replicas 不满足，可能影响写入
// - enable.idempotence=true、max.in.flight.requests.per.connection=1
//
// 如需修改连接配置（bootstrap.servers、SASL 等），请确认 conf/kafka.json 后再运行。
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkagodemo/config"
)

const (
	INT32_MAX = 2147483647 - 1000

	// 模拟的「订单」数量，用于演示局部顺序：相同 orderKey 的消息会落入同一分区
	numOrders = 3
)

// produceMeta 用于在 delivery callback 中关联发送时间，观测 max.in.flight 行为
type produceMeta struct {
	produceTime time.Time
	index       int
}

// doInitProducer 初始化支持消息顺序性的 Kafka 生产者
//
// 顺序性保证机制：
// 1. 精准路由：通过为消息指定 Key（如 orderID），Kafka 按 Key 哈希到固定分区，相同 Key 的消息保证局部顺序
// 2. 幂等性（enable.idempotence=true）：防止重试导致的消息重复与乱序，结合序列号保证写入顺序与发送顺序一致
// 3. 限制在途请求（max.in.flight.requests.per.connection=1）：在收到前一条 ACK 前不发送下一条，彻底避免重试乱序
// 4. acks=all：幂等性要求，确保 leader 与所有 in-sync 副本确认后才返回
func doInitProducer(cfg *config.KafkaConfig) *kafka.Producer {
	fmt.Print("init kafka producer, it may take a few seconds to init the connection\n")
	var kafkaconf = &kafka.ConfigMap{
		"api.version.request": "true",
		"message.max.bytes":   1000000,
		"retries":             INT32_MAX,
		"retry.backoff.ms":    1000,
		// "debug": "broker,topic,msg",
		// 禁用/减少批处理，让每条消息单独发送，便于观测 max.in.flight=1 效果
		"linger.ms":           0,   // 消息到达后立即发送，不等待合并
		"batch.num.messages": 1,    // 每条消息单独成批发送（librdkafka）
		// 顺序性相关配置
		"acks":                              "all", // 幂等性要求，必须为 all
		"enable.idempotence":                true,  // 开启幂等性，防止重试乱序与重复
		"max.in.flight.requests.per.connection": 1, // 未开启幂等时需为 1；开启后可为 1~5，1 最严格保证顺序
	}
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

	// Delivery report handler：记录 Produce 与 ACK 时间，用于观测 max.in.flight 行为
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Failed to write: %v", ev.TopicPartition.Error)
				} else {
					ackTime := time.Now()
					if meta, ok := ev.Opaque.(*produceMeta); ok {
						log.Printf("ACK #%d at %v (latency %v) topic:%v partition:%v offset:%v content:%s",
							meta.index, ackTime.Format("15:04:05.000"), ackTime.Sub(meta.produceTime),
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, ev.Value)
					} else {
						log.Printf("Send OK topic:%v partition:%v offset:%v content:%s",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset, ev.Value)
					}
				}
			}
		}
	}()

	// 顺序性演示：按「订单」分组发送，相同 orderKey 的消息会路由到同一分区，保证局部顺序
	// 每个订单发送多条消息（如：创建、支付、发货），这些消息在分区内按发送顺序存储
	maxMessages := 10
	for i := 0; i < maxMessages; i++ {
		orderID := i % numOrders
		seqInOrder := i/numOrders + 1
		orderKey := fmt.Sprintf("order-%d", orderID)
		value := fmt.Sprintf("order-%d seq-%d: kafka ordered message %d", orderID, seqInOrder, i+1)

		produceTime := time.Now()
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &cfg.Topic, Partition: kafka.PartitionAny},
			Key:            []byte(orderKey),
			Value:          []byte(value),
			Opaque:         &produceMeta{produceTime: produceTime, index: i},
		}
		log.Printf("Produce #%d at %v", i, produceTime.Format("15:04:05.000"))
		producer.Produce(msg, nil)
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	fmt.Printf("Successfully sent %d messages (ordered by key: order-0..order-%d)\n", maxMessages, numOrders-1)
	// Wait for message deliveries before shutting down // 理论上等待所有消息发送完成再退出（死循环下不会执行到这里）
	producer.Flush(15 * 1000) // 等待最长 15 秒以刷新缓冲区中的消息
}
