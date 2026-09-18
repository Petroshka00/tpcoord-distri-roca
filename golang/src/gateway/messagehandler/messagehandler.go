package messagehandler

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var clientCounter uint64

type MessageHandler struct {
	clientID string
}

func NewMessageHandler() MessageHandler {
	id := atomic.AddUint64(&clientCounter, 1)
	return MessageHandler{
		clientID: fmt.Sprintf("client-%d-%d", time.Now().UnixNano(), id),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(messageHandler.clientID, data, false)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(messageHandler.clientID, nil, true)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	clientID, fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if clientID != messageHandler.clientID {
		// Message belongs to another client, gateway should try other handlers
		return nil, nil
	}
	return fruitRecords, nil
}
