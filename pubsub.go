package galvin

import "sync"

type PubSub struct {
	sync.RWMutex
	subs map[string][]chan string
}

func NewPubSub() *PubSub {
	subs := make(map[string][]chan string)
	return &PubSub{
		subs: subs,
	}
}

func (ps *PubSub) Subscribe(topic string) <-chan string {
	ps.Lock()
	defer ps.Unlock()

	ch := make(chan string, 1)
	ps.subs[topic] = append(ps.subs[topic], ch)

	return ch
}

func (ps *PubSub) Publish(topic, msg string) {
	ps.RLock()
	defer ps.RUnlock()

	for _, ch := range ps.subs[topic] {
		ch <- msg
	}
}

func (ps *PubSub) CloseTopic(topic string) {
	ps.Lock()
	defer ps.Unlock()

	// Close the subscribing channels
	for _, ch := range ps.subs[topic] {
		close(ch)
	}
	// Delete the topic in our map
	delete(ps.subs, topic)
}
