package entity

type Message struct {
	Topic   string
	Content []byte
}

type ErrorProducing struct {
	Topic   string
	Content []byte
	Error   error
}
