package core

// import "fmt"
import (
	"encoding/json"
	// "reflect"
)

// 消息结构
type Message struct {
	Data           interface{} `json:"Data"`
	PublishTs      int64       `json:"PublishTs"`
	PublishTimeStr string      `json:"PublishTimeStr"`
	TaskId         string      `json:"TaskId"`
}

func (m *Message) ToJson() string {
	bytes, err := json.Marshal(m)
	if err != nil {
		return ""
	}
	return string(bytes)
}



type MessageWrapper struct {
	Msg          *Message               `json:"Msg"`
	ContextExtra map[string]interface{} `json:"ContextExtra"`
	JsonData     string                 `json:"JsonData"`
}

func  Json2Message(msgStr string) *Message {
	var msg Message
	err := json.Unmarshal([]byte(msgStr), &msg)
	if err!= nil {
		return nil
	}
	return &msg
}