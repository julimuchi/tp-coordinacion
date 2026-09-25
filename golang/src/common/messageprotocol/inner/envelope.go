package inner

import "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

func NewDataEnvelope(clientId string, data []fruititem.FruitItem) Envelope {
	return Envelope{
		ClientId: clientId,
		Type:     MessageTypeData,
		Data:     data,
	}
}

func NewEofEnvelope(clientId string) Envelope {
	return Envelope{
		ClientId: clientId,
		Type:     MessageTypeEOF,
		Data:     nil, //Obviamente NO es necesario, pero para que quede super visible
	}
}
