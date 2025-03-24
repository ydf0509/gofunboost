package core

// import "fmt"
import "encoding/json"

// 消息结构
type Message struct {
	FucnArgs []interface{} `json:"FucnArgs"`
	PublishTs int64 `json:"PublishTs"`
	PublishTimeStr string `json:"PublishTimeStr"`
	TaskId string `json:"TaskId"`
}



func (m *Message) ToJson() string {
	bytes, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(bytes)
}


type MessageWrapper struct {
	Msg *Message `json:"Msg"`
	ContextExtra map[string]interface{} `json:"ContextExtra"`
}

