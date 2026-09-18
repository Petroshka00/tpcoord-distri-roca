package join

import (
	"log/slog"
	"sort"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int
	clientTops        map[string][]fruititem.FruitItem
	clientEofCount    map[string]int
	mutex             sync.Mutex
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		clientTops:        map[string][]fruititem.FruitItem{},
		clientEofCount:    map[string]int{},
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message in join", "err", err)
		return
	}

	join.mutex.Lock()
	if !isEof {
		join.clientTops[clientID] = append(join.clientTops[clientID], fruitRecords...)
		join.mutex.Unlock()
		return
	}

	join.clientEofCount[clientID]++
	count := join.clientEofCount[clientID]
	if count < join.aggregationAmount {
		join.mutex.Unlock()
		return
	}

	allRecords := join.clientTops[clientID]
	delete(join.clientTops, clientID)
	delete(join.clientEofCount, clientID)
	join.mutex.Unlock()

	sort.SliceStable(allRecords, func(i, j int) bool {
		return allRecords[j].Less(allRecords[i])
	})
	finalTopSize := min(join.topSize, len(allRecords))
	finalTop := allRecords[:finalTopSize]

	resultMsg, err := inner.SerializeMessage(clientID, finalTop, false)
	if err != nil {
		slog.Error("While serializing final result in join", "err", err)
		return
	}

	if err := join.outputQueue.Send(*resultMsg); err != nil {
		slog.Error("While sending top to gateway", "err", err)
	}
}
