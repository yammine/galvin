package galvin

import "sync"

type PubSub struct {
	sync.RWMutex
	subs map[string]chan interface{}
}

func NewPubSub() *PubSub {
	subs := make(map[string]chan interface{})
	return &PubSub{
		subs: subs,
	}
}

func (ps *PubSub) Subscribe(topic string) <-chan interface{} {
	ps.Lock()
	defer ps.Unlock()

	ch := make(chan interface{}, 1)
	ps.subs[topic] = ch

	return ch
}

func (ps *PubSub) Publish(topic string, msg interface{}) {
	ps.RLock()
	defer ps.RUnlock()

	if ch, ok := ps.subs[topic]; ok {
		ch <- msg
	}
}

func (ps *PubSub) CloseTopic(topic string) {
	ps.Lock()
	defer ps.Unlock()

	// Close the subscribing channels
	if ch, ok := ps.subs[topic]; ok {
		close(ch)
		delete(ps.subs, topic)
	}
}
