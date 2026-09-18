package middleware

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueueMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueName   string
	consumerTag string
	consumerSeq int
	isConsuming bool
	manualStop  bool
}

func processDeliveries(deliveries <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) {
	for d := range deliveries {
		delivery := d
		ack := func() {
			_ = delivery.Ack(false) // confirma solo este mensaje
		}
		nack := func() {
			_ = delivery.Nack(false, true) // solo este mensaje, reintentar
		}

		callbackFunc(Message{Body: string(delivery.Body)}, ack, nack)
	}
}

func (r *RabbitMQQueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if r.isConsuming {
		return ErrMessageMiddlewareMessage
	}

	// Tag unico por consumer para poder cancelar la suscripcion
	r.consumerSeq++
	tag := fmt.Sprintf("q-cons-%s-%d", r.queueName, r.consumerSeq)
	r.consumerTag = tag
	r.isConsuming = true
	r.manualStop = false

	deliveries, err := r.ch.Consume(
		r.queueName,
		tag,
		false, // confirmacion manual con ack/nack
		false, // cola compartida entre varios workers
		false, // permitir recibir mensajes de la misma conexion
		false, // esperar confirmacion
		nil,
	)
	if err != nil {
		r.isConsuming = false
		r.consumerTag = ""
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	processDeliveries(deliveries, callbackFunc)

	// Para distinguir entre StopConsuming y si hubo error
	wasManual := r.manualStop
	r.isConsuming = false
	r.consumerTag = ""

	if wasManual {
		return nil
	}

	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return ErrMessageMiddlewareMessage
}

func (r *RabbitMQQueueMiddleware) StopConsuming() error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	if !r.isConsuming || r.consumerTag == "" {
		return nil
	}

	r.manualStop = true
	err := r.ch.Cancel(r.consumerTag, false)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (r *RabbitMQQueueMiddleware) Send(msg Message) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	err := r.ch.Publish(
		"",
		r.queueName,
		false, // no devolver error si no hay cola bound
		false, // no exigir consumidor inmediato
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		},
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (r *RabbitMQQueueMiddleware) Close() error {
	var closeErr error
	if r.ch != nil && !r.ch.IsClosed() {
		if err := r.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
	}
	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
	}
	return closeErr
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	_, err = ch.QueueDeclare(
		queueName,
		false, // cola no durable
		false, // no borrar la cola si los workers se desconectan
		false, // cola compartida entre varios workers
		false, // esperar confirmacion
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	// Fair Dispatch
	_ = ch.Qos(1, 0, false)

	return &RabbitMQQueueMiddleware{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

type RabbitMQExchangeMiddleware struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	exchangeName string
	routingKeys  []string
	queueName    string
	consumerTag  string
	consumerSeq  int
	isConsuming  bool
	manualStop   bool
}

func (r *RabbitMQExchangeMiddleware) setupSubscriberQueue() (string, error) {
	q, err := r.ch.QueueDeclare(
		"",    // nombre vacio, rabbitmq genera uno
		false, // cola no durable
		true,  // borrar la cola al desconectarse el suscriptor
		true,  // cola exclusiva
		false, // esperar confirmacion
		nil,
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return "", ErrMessageMiddlewareDisconnected
		}
		return "", ErrMessageMiddlewareMessage
	}

	keys := r.routingKeys
	if len(keys) == 0 {
		keys = []string{""}
	}

	// Vincular la queue del suscriptor a cada topic en el exchange.
	for _, key := range keys {
		err = r.ch.QueueBind(
			q.Name,
			key,
			r.exchangeName,
			false, // esperar confirmacion
			nil,
		)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
				return "", ErrMessageMiddlewareDisconnected
			}
			return "", ErrMessageMiddlewareMessage
		}
	}

	return q.Name, nil
}

func (r *RabbitMQExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	if r.isConsuming {
		return ErrMessageMiddlewareMessage
	}

	queueName, err := r.setupSubscriberQueue()
	if err != nil {
		return err
	}
	r.queueName = queueName

	r.consumerSeq++
	tag := fmt.Sprintf("ex-cons-%s-%d", r.exchangeName, r.consumerSeq)
	r.consumerTag = tag
	r.isConsuming = true
	r.manualStop = false

	deliveries, err := r.ch.Consume(
		r.queueName,
		tag,
		false, // confirmacion manual con ack/nack
		false, // cola compartida entre varios workers
		false, // permitir recibir mensajes de la misma conexion
		false, // esperar confirmacion
		nil,
	)
	if err != nil {
		r.isConsuming = false
		r.consumerTag = ""
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	processDeliveries(deliveries, callbackFunc)

	wasManual := r.manualStop
	r.isConsuming = false
	r.consumerTag = ""

	if wasManual {
		return nil
	}

	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	return ErrMessageMiddlewareMessage
}

func (r *RabbitMQExchangeMiddleware) StopConsuming() error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	if !r.isConsuming || r.consumerTag == "" {
		return nil
	}

	r.manualStop = true
	err := r.ch.Cancel(r.consumerTag, false) // esperar confirmacion para cancelar
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}

func (r *RabbitMQExchangeMiddleware) Send(msg Message) error {
	if r.conn == nil || r.conn.IsClosed() || r.ch == nil || r.ch.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	keys := r.routingKeys
	if len(keys) == 0 {
		keys = []string{""}
	}

	for _, key := range keys {
		err := r.ch.Publish(
			r.exchangeName,
			key,
			false, // no devolver error si no hay suscriptor en esa key
			false, // no exigir suscriptor inmediato
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
			},
		)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) || r.conn.IsClosed() || r.ch.IsClosed() {
				return ErrMessageMiddlewareDisconnected
			}
			return ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func (r *RabbitMQExchangeMiddleware) Close() error {
	var closeErr error
	if r.ch != nil && !r.ch.IsClosed() {
		if err := r.ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
	}
	if r.conn != nil && !r.conn.IsClosed() {
		if err := r.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
	}
	return closeErr
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, ErrMessageMiddlewareDisconnected
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false, // exchange no durable
		false, // no borrar el exchange si se desvinculan colas
		false, // permite que los productores publiquen directo
		false, // esperar confirmacion
		nil,
	)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, ErrMessageMiddlewareMessage
	}

	return &RabbitMQExchangeMiddleware{
		conn:         conn,
		ch:           ch,
		exchangeName: exchange,
		routingKeys:  keys,
	}, nil
}
