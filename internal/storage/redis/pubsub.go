package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Publisher publishes messages to Redis pub/sub channels.
type Publisher struct {
	client *Client
}

// NewPublisher creates a Publisher backed by the given Redis client.
func NewPublisher(client *Client) *Publisher {
	return &Publisher{client: client}
}

// Publish serializes data as JSON and publishes it to the named channel.
func (p *Publisher) Publish(ctx context.Context, channel string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("pubsub marshal: %w", err)
	}
	return p.client.rdb.Publish(ctx, channel, payload).Err()
}

// PublishRaw publishes raw bytes to the named channel without serialization.
func (p *Publisher) PublishRaw(ctx context.Context, channel string, data []byte) error {
	return p.client.rdb.Publish(ctx, channel, data).Err()
}

// Subscriber receives messages from Redis pub/sub channels.
type Subscriber struct {
	client *Client
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewSubscriber creates a Subscriber backed by the given Redis client.
func NewSubscriber(client *Client) *Subscriber {
	return &Subscriber{
		client: client,
	}
}

// Subscribe listens on the given channel and calls handler for each message received.
// The handler receives the raw message payload as bytes.
// Subscribe blocks until the context is cancelled or Close is called.
// It spawns an internal goroutine and returns immediately.
func (s *Subscriber) Subscribe(ctx context.Context, channel string, handler func([]byte)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	subCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})

	pubsub := s.client.rdb.Subscribe(subCtx, channel)

	// Wait for the subscription to be confirmed before returning.
	_, err := pubsub.Receive(subCtx)
	if err != nil {
		cancel()
		pubsub.Close()
		return fmt.Errorf("subscribe to %q: %w", channel, err)
	}

	ch := pubsub.Channel()

	go func() {
		defer close(s.done)
		defer pubsub.Close()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				handler([]byte(msg.Payload))
			case <-subCtx.Done():
				return
			}
		}
	}()

	return nil
}

// SubscribeMultiple listens on multiple channels and calls handler for each message.
// The handler receives the channel name and raw payload.
func (s *Subscriber) SubscribeMultiple(ctx context.Context, channels []string, handler func(channel string, data []byte)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	subCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})

	pubsub := s.client.rdb.Subscribe(subCtx, channels...)

	_, err := pubsub.Receive(subCtx)
	if err != nil {
		cancel()
		pubsub.Close()
		return fmt.Errorf("subscribe to channels: %w", err)
	}

	ch := pubsub.Channel()

	go func() {
		defer close(s.done)
		defer pubsub.Close()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				handler(msg.Channel, []byte(msg.Payload))
			case <-subCtx.Done():
				return
			}
		}
	}()

	return nil
}

// Close cancels the subscription and waits for the receiver goroutine to exit.
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.done != nil {
		<-s.done
		s.done = nil
	}
}
