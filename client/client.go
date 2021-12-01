package client

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Client struct {
	jsonClient     *JsonClient
	extraGenerator ExtraGenerator
	catcher        chan *Response
	listenerStore  *listenerStore
	catchersStore  *sync.Map
	updatesTimeout time.Duration
	catchTimeout   time.Duration
}

type Option func(*Client)

func WithExtraGenerator(extraGenerator ExtraGenerator) Option {
	return func(client *Client) {
		client.extraGenerator = extraGenerator
	}
}

func WithCatchTimeout(timeout time.Duration) Option {
	return func(client *Client) {
		client.catchTimeout = timeout
	}
}

func WithUpdatesTimeout(timeout time.Duration) Option {
	return func(client *Client) {
		client.updatesTimeout = timeout
	}
}

func WithProxy(req *AddProxyRequest) Option {
	return func(client *Client) {
		client.AddProxy(req)
	}
}

func WithLogVerbosity(req *SetLogVerbosityLevelRequest) Option {
	return func(client *Client) {
		client.SetLogVerbosityLevel(req)
	}
}

func NewClient(options ...Option) (*Client, error) {
	catchersListener := make(chan *Response, 1000)

	client := &Client{
		jsonClient:    NewJsonClient(),
		catcher:       catchersListener,
		listenerStore: newListenerStore(),
		catchersStore: &sync.Map{},
	}

	client.extraGenerator = UuidV4Generator()
	client.catchTimeout = 60 * time.Second
	client.updatesTimeout = 60 * time.Second

	for _, option := range options {
		option(client)
	}

	go receive(client)
	go client.catch(catchersListener)

	return client, nil
}

func (client *Client) Auth(ctx context.Context, authHandler AuthorizationStateHandler) error {
	return Authorize(ctx, client, authHandler)
}

var mutex = sync.RWMutex{}
var receiveStarted = false
var clients = make(map[int]*Client)

func receive(client *Client) {
	mutex.Lock()
	_, ok := clients[client.jsonClient.id]
	if !ok {
		clients[client.jsonClient.id] = client
	}
	if receiveStarted == true {
		//receiver already started in different thread
		return
	}
	receiveStarted = true
	mutex.Unlock()
	for {
		resp, err := Receive(client.updatesTimeout)
		if err != nil {
			continue
		}
		receivedClientId := resp.ClientId
		mutex.RLock()
		_, ok = clients[receivedClientId]
		if !ok {
			mutex.RUnlock()
			continue
		}

		receiverClient := clients[receivedClientId]

		receiverClient.catcher <- resp

		mutex.RUnlock()
		typ, err := UnmarshalType(resp.Data)
		if err != nil {
			continue
		}

		needGc := false
		for _, listener := range receiverClient.listenerStore.Listeners() {
			if listener.IsActive() {
				listener.Updates <- typ
			} else {
				needGc = true
			}
		}
		if needGc {
			receiverClient.listenerStore.gc()
		}
	}
}

func (client *Client) catch(updates chan *Response) {
	for update := range updates {
		if update.Extra != "" {
			value, ok := client.catchersStore.Load(update.Extra)
			if ok {
				value.(chan *Response) <- update
			}
		}
	}
}

func (client *Client) Send(req Request) (*Response, error) {
	req.Extra = client.extraGenerator()

	catcher := make(chan *Response, 1)

	client.catchersStore.Store(req.Extra, catcher)

	defer func() {
		client.catchersStore.Delete(req.Extra)
		close(catcher)
	}()

	client.jsonClient.Send(req)

	ctx, cancel := context.WithTimeout(context.Background(), client.catchTimeout)
	defer cancel()

	select {
	case response := <-catcher:
		return response, nil

	case <-ctx.Done():
		return nil, errors.New("response catching timeout")
	}
}

func (client *Client) GetListener() *Listener {
	listener := &Listener{
		isActive: true,
		Updates:  make(chan Type, 1000),
	}
	client.listenerStore.Add(listener)

	return listener
}

func (client *Client) Stop() {
	client.Destroy()
}
