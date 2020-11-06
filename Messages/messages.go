package messages

// Msg -> System level message format
type Msg struct {
	SrcAddr string
	ID      string
	Payload []byte
	Action  string
	Context map[string]int
}

// PayloadToStr -> Convert the message payload to a string
func (m *Msg) PayloadToStr() string {
	return string(m.Payload)
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
