package common

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	channel  *amqp.Channel
	name     string
	exchange string
}

func NewRabbitMQ(url string) *RabbitMQ {
	conn, e := amqp.Dial(url)
	if e != nil {
		panic(e)
	}

	ch, e := conn.Channel()
	if e != nil {
		panic(e)
	}

	// 每个新建的queue都会绑定到一个默认交换机上，binding key即是queue name
	q, e := ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if e != nil {
		panic(e)
	}

	mq := &RabbitMQ{
		channel: ch,
		name:    q.Name,
	}
	return mq
}

func (q *RabbitMQ) Bind(exchange string) {
	e := q.channel.QueueBind(
		q.name,
		"",
		exchange,
		false,
		nil)
	if e != nil {
		panic(e)
	}

	q.exchange = exchange
}

func (q *RabbitMQ) Send(queue string, body interface{}) {
	msgBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	err = q.channel.Publish(
		"",
		queue,
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.name,
			Body:    msgBody,
		})
	if err != nil {
		panic(err)
	}
}

func (q *RabbitMQ) Publish(exchange string, body interface{}) {
	msgBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	err = q.channel.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ReplyTo: q.name,
			Body:    msgBody,
		},
	)
	if err != nil {
		panic(err)
	}
}

func (q *RabbitMQ) Consume() <-chan amqp.Delivery {
	c, err := q.channel.Consume(
		q.name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	return c
}

func (q *RabbitMQ) Close() {
	q.channel.Close()
}
