package sum

import (
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
	aggregationAmount  int
	inputQueue         middleware.Middleware
	outputExchanges    []middleware.Middleware
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

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		routingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		ex, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, routingKey, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				outputExchanges[j].Close()
			}
			return nil, err
		}
		outputExchanges[i] = ex
	}

	var controlConsumer middleware.Middleware
	var controlPublisher middleware.Middleware

	if config.SumAmount > 1 {
		controlExchangeName := fmt.Sprintf("%s_control", config.SumPrefix)
		myKey := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}

		controlConsumer, err = middleware.CreateExchangeMiddleware(controlExchangeName, myKey, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, ex := range outputExchanges {
				ex.Close()
			}
			return nil, err
		}

		allKeys := make([]string, config.SumAmount)
		for i := range config.SumAmount {
			allKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
		}

		controlPublisher, err = middleware.CreateExchangeMiddleware(controlExchangeName, allKeys, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, ex := range outputExchanges {
				ex.Close()
			}
			controlConsumer.Close()
			return nil, err
		}
	}

	return &Sum{
		id:                 config.Id,
		sumAmount:          config.SumAmount,
		aggregationAmount:  config.AggregationAmount,
		inputQueue:         inputQueue,
		outputExchanges:    outputExchanges,
		controlConsumer:    controlConsumer,
		controlPublisher:   controlPublisher,
		clientFruitItemMap: map[string]map[string]fruititem.FruitItem{},
		clientFinished:     map[string]bool{},
	}, nil
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	if sum.controlConsumer != nil {
		_ = sum.controlConsumer.StopConsuming()
	}
	_ = sum.inputQueue.StopConsuming()
}

func (sum *Sum) Close() error {
	var errs []error
	if sum.controlConsumer != nil {
		if err := sum.controlConsumer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if sum.controlPublisher != nil {
		if err := sum.controlPublisher.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := sum.inputQueue.Close(); err != nil {
		errs = append(errs, err)
	}
	for _, ex := range sum.outputExchanges {
		if err := ex.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (sum *Sum) Run() {
	go sum.handleSignals()

	var wg sync.WaitGroup
	if sum.controlConsumer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sum.controlConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
				sum.handleControlMessage(msg, ack, nack)
			}); err != nil {
				slog.Error("Control consumer stopped", "err", err)
			}
		}()
	}

	if err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("Input queue consumer stopped", "err", err)
	}

	wg.Wait()
	if err := sum.Close(); err != nil {
		slog.Error("Error closing sum resources", "err", err)
	}
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

			targetIndex := crc32.ChecksumIEEE([]byte(fruits[key].Fruit)) % uint32(sum.aggregationAmount)
			if err := sum.outputExchanges[targetIndex].Send(*message); err != nil {
				slog.Debug("While sending partitioned message", "err", err)
				return err
			}
		}
	}

	message, err := inner.SerializeMessage(clientID, nil, true)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for _, exchange := range sum.outputExchanges {
		if err := exchange.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
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
