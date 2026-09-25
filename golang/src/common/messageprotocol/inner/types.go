package inner

import "github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

const (
	MessageTypeData         string = "DATA"
	MessageTypeEOF          string = "EOF"
	MessageTypeBroadcastEOF string = "BRODCAST_EOF"
)

type Envelope struct {
	ClientId string                `json:"client_id"`
	Type     string                `json:"type"`
	Data     []fruititem.FruitItem `json:"data"`
}
