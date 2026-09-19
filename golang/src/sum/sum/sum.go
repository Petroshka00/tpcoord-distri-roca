package sum

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                 int
	sumAmount          int
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	controlConsumer    middleware.Middleware
	controlPublisher   middleware.Middleware
	clientFruitItemMap map[string]map[string]fruititem.FruitItem
	clientFinished     map[string]bool
	mutex              sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	var controlConsumer middleware.Middleware
	var controlPublisher middleware.Middleware

	if config.SumAmount > 1 {
		controlExchangeName := fmt.Sprintf("%s_control", config.SumPrefix)
		myKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}

		controlConsumer, err = middleware.CreateExchangeMiddleware(controlExchangeName, myKey, connSettings)
		if err != nil {
			inputQueue.Close()
			outputExchange.Close()
			return nil, err
		}

		allKeys := make([]string, config.SumAmount)
		for i := range config.SumAmount {
			allKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
		}

		controlPublisher, err = middleware.CreateExchangeMiddleware(controlExchangeName, allKeys, connSettings)
		if err != nil {
			inputQueue.Close()
			outputExchange.Close()
			controlConsumer.Close()
			return nil, err
		}
	}

	return &Sum{
		id:                 config.Id,
		sumAmount:          config.SumAmount,
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		controlConsumer:    controlConsumer,
		controlPublisher:   controlPublisher,
		clientFruitItemMap: map[string]map[string]fruititem.FruitItem{},
		clientFinished:     map[string]bool{},
	}, nil
}

func (sum *Sum) Run() {
	if sum.controlConsumer != nil {
		go func() {
			if err := sum.controlConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
				sum.handleControlMessage(msg, ack, nack)
			}); err != nil {
				slog.Error("Control consumer stopped", "err", err)
			}
		}()
	}

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, _, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	}

	if isEof {
		if err := sum.handleControlEOF(clientID); err != nil {
			slog.Error("While handling control EOF", "err", err)
		}
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if sum.sumAmount > 1 && sum.controlPublisher != nil {
			slog.Info("Broadcasting EOF to all sum instances", "sumID", sum.id, "clientID", clientID)
			broadcastMsg, err := inner.SerializeMessage(clientID, nil, true)
			if err != nil {
				slog.Error("While serializing broadcast EOF", "err", err)
				return
			}
			if err := sum.controlPublisher.Send(*broadcastMsg); err != nil {
				slog.Error("While broadcasting EOF message", "err", err)
			}
		} else {
			if err := sum.handleControlEOF(clientID); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
		}
		return
	}

	if err := sum.handleDataMessage(clientID, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleControlEOF(clientID string) error {
	slog.Info("Handling EOF for client", "sumID", sum.id, "clientID", clientID)

	sum.mutex.Lock()
	if sum.clientFinished[clientID] {
		sum.mutex.Unlock()
		return nil
	}
	sum.clientFinished[clientID] = true
	fruits, ok := sum.clientFruitItemMap[clientID]
	delete(sum.clientFruitItemMap, clientID)
	sum.mutex.Unlock()

	if ok {
		for key := range fruits {
			fruitRecord := []fruititem.FruitItem{fruits[key]}
			message, err := inner.SerializeMessage(clientID, fruitRecord, false)
			if err != nil {
				slog.Debug("While serializing message", "err", err)
				return err
			}
			if err := sum.outputExchange.Send(*message); err != nil {
				slog.Debug("While sending message", "err", err)
				return err
			}
		}
	}

	message, err := inner.SerializeMessage(clientID, nil, true)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if sum.clientFinished[clientID] {
		return nil
	}

	fruits, ok := sum.clientFruitItemMap[clientID]
	if !ok {
		fruits = make(map[string]fruititem.FruitItem)
		sum.clientFruitItemMap[clientID] = fruits
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := fruits[fruitRecord.Fruit]
		if ok {
			fruits[fruitRecord.Fruit] = fruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			fruits[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
