// Package main 定义主包，作为程序的入口点
package main

// Import 导入所需的依赖包
import (
	"fmt"  // 格式化输入输出包，用于打印日志和信息
	"time" // 时间包，用于延迟显示信息

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"  // Confluent Kafka Go 客户端库，提供 Kafka 消费者功能
	"kafkagodemo/config"  // 项目内部的配置包，用于加载 Kafka 配置文件
)

// doInitConsumer 初始化 Kafka 消费者客户端
// 参数 cfg: KafkaConfig 结构体指针，包含 Kafka 连接和认证配置
// 参数 consumerName: 消费者名称标识
// 返回：*kafka.Consumer 初始化成功的 Kafka 消费者实例
func doInitConsumer(cfg *config.KafkaConfig, consumerName string) *kafka.Consumer {
	fmt.Printf("init kafka consumer [%s], it may take a few seconds to init the connection\n", consumerName)  // 打印初始化开始信息，提示用户连接初始化可能需要几秒
	
	// common arguments  // 注释：以下是 Kafka 消费者的通用配置参数
	var kafkaconf = &kafka.ConfigMap{  // 创建 Kafka 配置映射对象，用于存储所有配置键值对
		"api.version.request": "true",  // 请求 broker 支持的 API 版本，以便自动调整功能
		"auto.offset.reset": "earliest",  // 当消费者组没有之前的 offset 时，从最早的可用消息开始消费（earliest=最早，latest=最新）
		"heartbeat.interval.ms": 3000,  // 心跳间隔（毫秒），消费者向 broker 发送心跳的频率，用于检测消费者是否存活
		"session.timeout.ms": 30000,  // 会话超时时间（毫秒），broker 等待消费者心跳的最长时间，超时后认为消费者已下线
		"max.poll.interval.ms": 120000,  // 两次 poll 之间的最大间隔（毫秒），超过此时间未调用 ReadMessage 则认为消费者失败
		"fetch.max.bytes": 1024000,  // 每次从 broker 获取的最大字节数，控制单次网络请求的数据量
		"max.partition.fetch.bytes": 256000}  // 每个分区每次获取的最大字节数，限制单个分区的拉取数据量
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

// runConsumer 运行单个消费者协程
// 参数 consumer: Kafka 消费者实例
// 参数 consumerName: 消费者名称标识
// 参数 topic: 要订阅的主题名称
func runConsumer(consumer *kafka.Consumer, consumerName string, topic string) {
	// 定义 Rebalance 回调函数，用于显示分区分配情况
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
	
	consumer.SubscribeTopics([]string{topic}, rebalanceCb)  // 订阅单个主题，使用自定义的 Rebalance 回调
	
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("[消费者：%s] ✅ Message on Topic[%s] Partition[%d] Offset[%d]: %s\n", 
				consumerName, 
				msg.TopicPartition.Topic, 
				msg.TopicPartition.Partition, 
				msg.TopicPartition.Offset, 
				string(msg.Value))
		} else {
			// The client will
			//automatically try to recover from all errors.
			fmt.Printf("[消费者：%s] ❌ Consumer error: %v (%v)\n", consumerName, err, msg)
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
	
	// 启动第 1 个消费者组的 2 个消费者
	fmt.Println("\n--- Starting Consumer Group 1 ---")
	group1Consumer1 := doInitConsumer(cfg, "Group1-消费者 1")
	group1Consumer2 := doInitConsumer(cfg, "Group1-消费者 2")
	go runConsumer(group1Consumer1, "Group1-消费者 1", cfg.Topic)
	go runConsumer(group1Consumer2, "Group1-消费者 2", cfg.Topic)
	
	// 启动第 2 个消费者组的 2 个消费者（使用配置文件中的 group.id2）
	var group2Consumer1, group2Consumer2 interface{}
	if cfg.GroupId2 != "" {
		fmt.Println("\n--- Starting Consumer Group 2 ---")
		cfg2 := *cfg  // 复制配置
		cfg2.GroupId = cfg.GroupId2  // 使用配置文件中的第二个 group.id
		group2Consumer1 = doInitConsumer(&cfg2, "Group2-消费者 1")
		group2Consumer2 = doInitConsumer(&cfg2, "Group2-消费者 2")
		go runConsumer(group2Consumer1.(*kafka.Consumer), "Group2-消费者 1", cfg.Topic)
		go runConsumer(group2Consumer2.(*kafka.Consumer), "Group2-消费者 2", cfg.Topic)
		fmt.Printf("Configured Group2 ID: %s\n", cfg.GroupId2)
	} else {
		fmt.Println("\n--- No Group 2 configured (group.id2 not set in config) ---")
	}
	
	fmt.Println("\nAll consumers started, waiting for partition assignment...")
	
	// 等待 5 秒让 rebalance 完成，然后显示每个消费者的分区分配情况
	time.Sleep(3 * time.Second)
	
	fmt.Printf("\n=== Final Partition Assignment After Rebalance ===\n")
	
	// Group 1 的分区分配
	assignment1_1, _ := group1Consumer1.Assignment()
	assignment1_2, _ := group1Consumer2.Assignment()
	fmt.Printf("\n【Consumer Group 1】 (group.id: %s)\n", cfg.GroupId)
	fmt.Printf("  [Group1-消费者 1] Assigned partitions: %v (count: %d)\n", assignment1_1, len(assignment1_1))
	fmt.Printf("  [Group1-消费者 2] Assigned partitions: %v (count: %d)\n", assignment1_2, len(assignment1_2))
	
	// Group 2 的分区分配（如果配置了）
	if cfg.GroupId2 != "" {
		assignment2_1, _ := group2Consumer1.(*kafka.Consumer).Assignment()
		assignment2_2, _ := group2Consumer2.(*kafka.Consumer).Assignment()
		fmt.Printf("\n【Consumer Group 2】 (group.id: %s)\n", cfg.GroupId2)
		fmt.Printf("  [Group2-消费者 1] Assigned partitions: %v (count: %d)\n", assignment2_1, len(assignment2_1))
		fmt.Printf("  [Group2-消费者 2] Assigned partitions: %v (count: %d)\n", assignment2_2, len(assignment2_2))
	}
	
	fmt.Printf("\n==================================================\n\n")
	
	// 阻塞主协程，防止程序退出
	select {}
}