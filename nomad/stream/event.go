package stream

type Event struct {
	Topic   string
	Key     string
	Index   uint64
	Payload interface{}
}
