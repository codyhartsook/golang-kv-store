package messages

import (
	"fmt"
	"io"
	"io/ioutil"
)

// Msg -> System level message format
type Msg struct {
	SrcAddr string
	ID      string
	Payload io.Reader
	Action  string
	Context map[string]int
}

// PayloadToStr -> Convert the message payload to a string
func (m *Msg) PayloadToStr() string {
	buf, err := ioutil.ReadAll(m.Payload)

	if err != nil {
		fmt.Printf("error in reading message payload %v", err)
	}
	return string(buf)
}

// Entry -> format key-value user-level entries
type Entry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Key ->
type Key struct {
	Key string `json:"Key"`
}

// Value ->
type Value struct {
	Value string `json:"Value"`
}
