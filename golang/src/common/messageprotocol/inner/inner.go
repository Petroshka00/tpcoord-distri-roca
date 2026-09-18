package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(clientID string, fruitRecords []fruititem.FruitItem, isEOF bool) (*middleware.Message, error) {
	records := []interface{}{}
	for _, fruitRecord := range fruitRecords {
		datum := []interface{}{
			fruitRecord.Fruit,
			fruitRecord.Amount,
		}
		records = append(records, datum)
	}

	data := []interface{}{
		clientID,
		isEOF,
		records,
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (string, []fruititem.FruitItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	if len(data) < 3 {
		return "", nil, false, errors.New("Data is not a valid message envelope")
	}

	clientID, ok := data[0].(string)
	if !ok {
		return "", nil, false, errors.New("clientID is not a string")
	}

	isEOF, ok := data[1].(bool)
	if !ok {
		return "", nil, false, errors.New("isEOF is not a bool")
	}

	records, ok := data[2].([]interface{})
	if !ok {
		return "", nil, false, errors.New("Records is not an array")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range records {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return "", nil, false, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return "", nil, false, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return clientID, fruitRecords, isEOF, nil
}
