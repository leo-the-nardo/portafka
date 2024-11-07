package entity

// ChannelHolder is a struct that holds channels that needs to be used by multiple entities
type ChannelHolder struct {
	MessageCh chan Message
}
