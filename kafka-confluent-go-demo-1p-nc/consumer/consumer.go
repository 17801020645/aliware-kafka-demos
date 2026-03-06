// Package main 定义主包，作为程序的入口点
package main

// Import 导入所需的依赖包
import (
	"fmt"  // 格式化输入输出包，用于打印日志和信息

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"  // Confluent Kafka Go 客户端库，提供 Kafka 消费者功能
	"kafkagodemo/config"  // 项目内部的配置包，用于加载 Kafka 配置文件
)

// doInitConsumer 初始化 Kafka 消费者客户端
// 参数 cfg: KafkaConfig 结构体指针，包含 Kafka 连接和认证配置
// 返回：*kafka.Consumer 初始化成功的 Kafka 消费者实例
func doInitConsumer(cfg *config.KafkaConfig) *kafka.Consumer {
	fmt.Print("init kafka consumer, it may take a few seconds to init the connection\n")  // 打印初始化开始信息，提示用户连接初始化可能需要几秒
	
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
	fmt.Print("init kafka consumer success\n")  // 打印初始化成功信息
	return consumer;  // 返回初始化好的消费者实例
}

// main 程序主入口函数，启动 Kafka 消费者示例
func main() {

	// Choose the correct protocol  // 注释：选择正确的安全协议
	// 9092 for PLAINTEXT  // 注释：9092 端口用于明文传输协议
	// 9093 for SASL_SSL, need to provide sasl.username and sasl.password  // 注释：9093 端口用于 SASL_SSL 协议，需要提供用户名和密码
	// 9094 for SASL_PLAINTEXT, need to provide sasl.username and sasl.password  // 注释：9094 端口用于 SASL_PLAINTEXT 协议，需要提供用户名和密码
	cfg := config.MustLoad("")  // 从默认路径（conf/kafka.json）加载 Kafka 配置，加载失败会直接 panic
	consumer := doInitConsumer(cfg)  // 基于配置初始化 Kafka 消费者客户端

	consumer.SubscribeTopics([]string{cfg.Topic, cfg.Topic2}, nil)  // 订阅多个主题（topic 和 topic2），第二个参数 nil 表示使用默认的 Rebalance 回调

	for {  // 无限循环，持续从 Kafka 消费消息
		msg, err := consumer.ReadMessage(-1)  // 阻塞式读取消息，-1 表示无限期等待直到有消息到达
		if err == nil {  // 如果没有错误（成功读取到消息）
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))  // 打印消息所在的分区信息和消息内容
		} else {  // 如果读取消息时发生错误
			// The client will  // 注释：Kafka 客户端会自动尝试从所有错误中恢复
			//automatically try to recover from all errors.  // 注释：无需手动处理，客户端内部会重试
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)  // 打印消费者错误信息和相关消息
		}
	}

	consumer.Close()  // 注意：这行代码不会被执行到，因为上面是无限循环，实际使用时需要在合适的地方 break 退出循环
}