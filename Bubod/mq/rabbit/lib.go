/**
* rabbitmq
* type Work queues
* message queue
*/
package rabbit

import (
	// "bytes"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strings"
	// "time"
	"bytes"
)

// 必要方法
type MqClass interface {
	// 连接
	Connect() (error)
	// push
	Push(string) (error)
	// pop
	// Pop(string, func(string) bool)
}
 
type Mq struct {
	Amqp 	string
	Qname	string		 	// 队列name
	Conn 	*amqp.Connection
	Channel *amqp.Channel
	// Callback 回调函数
 }

 
// 回调函数
// type CallbackFunc func(string) bool
// var queueDeclare *amqp.QueueDeclare

/**
* [NewClient 创建连接]
* 方法实现
* @param {[type]} ) (*redis.Client, error [description]
*/
func (mq *Mq) Connect() error {
	err := mq.initConnection()
	return err
}

func (mq *Mq) initConnection() error {

	if mq.Conn == nil {
		conn, err := amqp.Dial(mq.Amqp)
		if err != nil {
			log.Println("[error] Failed to connect to RabbitMQ error:", err)
			defer conn.Close()
			return err
		}
		mq.Conn = conn
		channel, err := conn.Channel()
		if err != nil {
			log.Println("[error] Failed to open a channel error:", err)
			defer channel.Close()
			return err
		}
		mq.Channel = channel
		// queueDeclare, err = channel.QueueDeclare(
		// 	queueName, // name
		// 	true,      // durable
		// 	false,     // delete when unused
		// 	false,     // exclusive
		// 	false,     // no-wait
		// 	nil,       // arguments
		// )
		// failOnError(err, "Failed to declare a queue")
	}
	return nil
}
 
/**
* [pop_data push数据]
* data json string
* @return {[type]} [description]
*/
func (mq *Mq) Push(data string) error {

	if mq.Channel == nil {
		mq.Connect()
	}
	err := mq.Channel.Publish(
		"",        // exchange
		mq.Qname, // queueDeclare.Name, // routing key
		false,     // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(data),
		})
	// failOnError(err, "Failed to publish a message")
	return err
}
 
/**
* [pop_data 消费数据]
* @return {[type]} [description]

func (MqClass *MqClass) Pop_data(queueName string, Callback func(string) bool) {

	if channel == nil {
		MqClass.Connect()
	}

	err := channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := channel.Consume(
		queueName, //q.Name, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {

		// 使用callback消费数据
		for msg := range msgs {
			log.Printf("Received a message: %s", msg.Body)

			body := BytesToString(&(msg.Body))

			// 当接收者消息处理失败的时候，
			// 比如网络问题导致的数据库连接失败，redis连接失败等等这种
			// 通过重试可以成功的操作，那么这个时候是需要重试的
			// 直到数据处理成功后再返回，然后才会回复rabbitmq ack
			
						for !CommonFunc.Consu_data(body) {
							// log.Warnf("receiver 数据处理失败，将要重试")
							time.Sleep(1 * time.Second)
						}
						

			// 不重试
			ret := Callback(*body)
			if ret {

			}

			// 确认收到本条消息, multiple必须为false
			msg.Ack(false)
		}
	}()

	// log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
*/

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

/**
* byte 转 string
*/
func BytesToString(b *[]byte) *string {
	s := bytes.NewBuffer(*b)
	r := s.String()
	return &r
}
 