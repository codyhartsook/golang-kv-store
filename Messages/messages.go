package messages

// Msg -> System level message format
type Msg struct {
	SrcAddr string
	ID      string
	Payload string
	Action  string
	Context map[string]int
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
