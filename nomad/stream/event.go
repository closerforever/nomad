package stream

const (
	AllKeys = "*"
)

type Topic string
type Key string

type Event struct {
	Topic   Topic
	Key     string
	Index   uint64
	Payload interface{}
}
