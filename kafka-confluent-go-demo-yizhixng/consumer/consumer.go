// 顺序性 Consumer 示例
//
// 保证消费顺序的要点：
// 1. 分区独占：同一消费者组内，一个分区只由一个消费者消费（Rebalance 自动保证）
//    建议：消费者数量 <= Topic 分区数，否则部分消费者会空闲
// 2. 单线程处理：每个消费者串行处理其分配到的分区，避免并发乱序
// 3. 处理完再提交：enable.auto.commit=false，仅在 processMessage 成功后才 CommitMessage
//    若先提交再处理，处理失败时会导致消息被跳过、顺序断裂
//
// 配置变更提醒：enable.auto.commit 在代码内设置，未修改 conf/kafka.json
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"kafkagodemo/config"
)

// doInitConsumer 初始化 Kafka 消费者客户端
// 参数 cfg: KafkaConfig 结构体指针，包含 Kafka 连接和认证配置
// 参数 consumerName: 消费者名称标识
// 返回：*kafka.Consumer 初始化成功的 Kafka 消费者实例
func doInitConsumer(cfg *config.KafkaConfig, consumerName string) *kafka.Consumer {
	fmt.Printf("init kafka consumer [%s], it may take a few seconds to init the connection\n", consumerName)  // 打印初始化开始信息，提示用户连接初始化可能需要几秒
	
	var kafkaconf = &kafka.ConfigMap{
		"api.version.request":       "true",
		"auto.offset.reset":         "earliest",
		"heartbeat.interval.ms":     3000,
		"session.timeout.ms":        30000,
		"max.poll.interval.ms":      120000,
		"fetch.max.bytes":           1024000,
		"max.partition.fetch.bytes": 256000,
		// 顺序性：关闭自动提交，处理完成后再手动提交，避免处理失败时跳过消息
		"enable.auto.commit": false,
	}
	kafkaconf.SetKey("bootstrap.servers", cfg.BootstrapServers);  // 设置 Kafka 集群的连接地址列表，从配置文件中读取
	kafkaconf.SetKey("group.id", cfg.GroupId)  // 设置消费者组 ID，同一组内的消费者共同消费分区

	// switch 语句根据安全协议类型配置不同的认证参数
	switch cfg.SecurityProtocol {
	case "PLAINTEXT" :  // 如果安全协议是明文传输（无加密无认证）
		kafkaconf.SetKey("security.protocol", "plaintext");  // 设置协议为 plaintext，不使用任何安全措施
	case "SASL_SSL":  // 如果安全协议是 SASL 认证 +SSL 加密（最安全的配置）
		kafkaconf.SetKey("security.protocol", "sasl_ssl")  // 设置协议为 sasl_ssl，启用 SASL 认证和 SSL 加密
		kafkaconf.SetKey("ssl.ca.location", cfg.SslCaLocation)  // 设置 SSL CA 证书路径，用于验证 broker 的证书
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername);  // 设置 SASL 用户名，用于身份认证
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword);  // 设置 SASL 密码，用于身份认证
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism);  // 设置 SASL 机制（如 PLAIN、SCRAM 等）
        // hostname 校验改成空  // 注释：禁用主机名验证，允许证书的主机名与实际连接的主机名不匹配
		kafkaconf.SetKey("ssl.endpoint.identification.algorithm", "None");  // 禁用 SSL 端点标识算法，不进行主机名验证
		kafkaconf.SetKey("enable.ssl.certificate.verification", "false");  // 禁用 SSL 证书验证，接受自签名证书
	case "SASL_PLAINTEXT":  // 如果安全协议是 SASL 认证 + 明文传输（有认证无加密）
		kafkaconf.SetKey("security.protocol", "sasl_plaintext");  // 设置协议为 sasl_plaintext，只启用 SASL 认证
		kafkaconf.SetKey("sasl.username", cfg.SaslUsername);  // 设置 SASL 用户名
		kafkaconf.SetKey("sasl.password", cfg.SaslPassword);  // 设置 SASL 密码
		kafkaconf.SetKey("sasl.mechanism", cfg.SaslMechanism)  // 设置 SASL 机制

	default:  // 如果配置的安全协议不在上述三种之中
		panic(kafka.NewError(kafka.ErrUnknownProtocol, "unknown protocol", true))  // 抛出未知协议错误，终止程序
	}

	consumer, err := kafka.NewConsumer(kafkaconf)  // 使用配置创建新的 Kafka 消费者实例
	if err != nil {  // 如果创建消费者时发生错误
		panic(err)  // 抛出致命错误并终止程序
	}
	fmt.Printf("init kafka consumer [%s] success\n", consumerName)  // 打印初始化成功信息
	return consumer;  // 返回初始化好的消费者实例
}

// processMessage 模拟业务处理，返回是否成功。
// 实际业务中可替换为数据库写入、RPC 调用等；失败时不提交 offset，消息会被重新消费。
func processMessage(consumerName string, msg *kafka.Message) bool {
	// 模拟业务逻辑：打印即视为处理
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	fmt.Printf("[消费者：%s] ✅ Message on Topic[%s] Partition[%d] Offset[%d]: %s\n",
		consumerName, topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value))
	return true
}

// runConsumer 运行单个消费者协程
// 单线程串行处理，保证分区内消息顺序；处理完成后再提交 offset。
func runConsumer(consumer *kafka.Consumer, consumerName string, topic string) {
	rebalanceCb := func(c *kafka.Consumer, event kafka.Event) error {
		switch ev := event.(type) {
		case kafka.AssignedPartitions:
			fmt.Printf("[消费者：%s] Partitions assigned: %v\n", consumerName, ev.Partitions)
			for _, p := range ev.Partitions {
				fmt.Printf("[消费者：%s]   - Partition %d will be consumed\n", consumerName, p.Partition)
			}
			c.Assign(ev.Partitions)
		case kafka.RevokedPartitions:
			fmt.Printf("[消费者：%s] Partitions revoked: %v\n", consumerName, ev.Partitions)
			c.Unassign()
		}
		return nil
	}

	consumer.SubscribeTopics([]string{topic}, rebalanceCb)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			if msg != nil && msg.TopicPartition.Topic != nil {
				fmt.Printf("[消费者：%s] ❌ Consumer error: %v (topic=%s partition=%d)\n",
					consumerName, err, *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
			} else {
				fmt.Printf("[消费者：%s] ❌ Consumer error: %v\n", consumerName, err)
			}
			continue
		}

		// 【处理完再提交】先执行业务逻辑，成功后才 CommitMessage；失败则不提交，消息会被重新消费
		if processMessage(consumerName, msg) {
			_, commitErr := consumer.CommitMessage(msg)
			if commitErr != nil {
				log.Printf("[消费者：%s] ⚠️ Commit failed (offset %d): %v，下次将重复消费", consumerName, msg.TopicPartition.Offset, commitErr)
			}
		} else {
			// 处理失败，不提交 offset；重启或下次 poll 时会从上次未提交位置重新拉取
			log.Printf("[消费者：%s] ⚠️ Process failed, skip commit for offset %d", consumerName, msg.TopicPartition.Offset)
		}
	}
}

// main 程序主入口函数，启动 Kafka 消费者示例
func main() {

	// Choose the correct protocol
	// 9092 for PLAINTEXT
	// 9093 for SASL_SSL, need to provide sasl.username and sasl.password
	// 9094 for SASL_PLAINTEXT, need to provide sasl.username and sasl.password
	cfg := config.MustLoad("")
	
	fmt.Println("========================================")
	fmt.Printf("Starting Kafka Consumer Groups\n")
	fmt.Printf("Subscribing to topic: %s\n", cfg.Topic)
	fmt.Println("========================================")
	
	// 启动消费者组的 2 个消费者
	fmt.Println("\n--- Starting Consumer Group ---")
	consumer1 := doInitConsumer(cfg, "消费者 1")
	consumer2 := doInitConsumer(cfg, "消费者 2")
	go runConsumer(consumer1, "消费者 1", cfg.Topic)
	go runConsumer(consumer2, "消费者 2", cfg.Topic)
	
	fmt.Println("\nAll consumers started, waiting for partition assignment...")
	
	// 等待 5 秒让 rebalance 完成，然后显示每个消费者的分区分配情况
	time.Sleep(3 * time.Second)
	
	fmt.Printf("\n=== Final Partition Assignment After Rebalance ===\n")
	
	assignment1, _ := consumer1.Assignment()
	assignment2, _ := consumer2.Assignment()
	fmt.Printf("\n【Consumer Group】 (group.id: %s)\n", cfg.GroupId)
	fmt.Printf("  [消费者 1] Assigned partitions: %v (count: %d)\n", assignment1, len(assignment1))
	fmt.Printf("  [消费者 2] Assigned partitions: %v (count: %d)\n", assignment2, len(assignment2))
	
	fmt.Printf("\n==================================================\n\n")
	
	// 阻塞主协程，防止程序退出
	select {}
}