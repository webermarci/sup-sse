package sse

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Observer defines the interface for receiving SSE events and connection status updates.
type Observer interface {
	OnConnect(url string, lastEventID string)
	OnEvent(event Event, duration time.Duration)
	OnFailure(err error)
}

// Event represents a single SSE event with its ID, data, and optional name.
type Event struct {
	ID   string
	Data string
	Name string
}

// SSEActor connects to an SSE endpoint, processes incoming events, and notifies an optional observer about connection status and received events.
type SSEActorOption func(*SSEActor)

// WithTimeout sets the duration after which the SSE connection will be considered timed out if no events are received.
func WithTimeout(d time.Duration) SSEActorOption {
	return func(a *SSEActor) {
		a.timeout = d
	}
}

// WithObserver assigns an Observer to the SSEActor, allowing it to receive notifications about connection status and events.
func WithObserver(observer Observer) SSEActorOption {
	return func(a *SSEActor) {
		a.observer = observer
	}
}

// WithHTTPClient allows the caller to provide a custom http.Client for making requests to the SSE endpoint, enabling configuration of timeouts, transport settings, etc.
func WithHTTPClient(c *http.Client) SSEActorOption {
	return func(a *SSEActor) {
		a.client = c
	}
}

// SSEActor is responsible for connecting to an SSE endpoint, reading and parsing incoming events, and invoking a handler function for each event. It also supports notifying an optional Observer about connection status and received events.
type SSEActor struct {
	url      string
	timeout  time.Duration
	lastID   string
	client   *http.Client
	handler  func(Event)
	observer Observer
}

// NewSSEActor creates a new SSEActor with the specified URL, event handler, and optional configuration options. The handler function will be called for each received event, and the options can be used to customize the connection timeout, assign an Observer, or provide a custom HTTP client.
func NewSSEActor(url string, handler func(Event), opts ...SSEActorOption) *SSEActor {
	a := &SSEActor{
		url:     url,
		handler: handler,
		timeout: 30 * time.Second,
		client: &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives: true,
			},
		},
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

// Run establishes a connection to the SSE endpoint and processes incoming events until the context is canceled or an error occurs. It handles connection setup, event parsing, and error handling, while also notifying the Observer about connection status and received events.
func (a *SSEActor) Run(ctx context.Context) (err error) {
	if a.observer != nil {
		a.observer.OnConnect(a.url, a.lastID)
	}

	defer func() {
		if err != nil && a.observer != nil {
			a.observer.OnFailure(err)
		}
	}()

	req, err := http.NewRequestWithContext(ctx, "GET", a.url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")
	if a.lastID != "" {
		req.Header.Set("Last-Event-ID", a.lastID)
	}

	client := a.client
	if client == nil {
		client = &http.Client{}
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return errors.New("unexpected status code: " + strconv.Itoa(res.StatusCode))
	}

	scanner := bufio.NewScanner(res.Body)
	var timedOut int32

	var buf bytes.Buffer
	var currentEvent Event
	eventStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		atomic.StoreInt32(&timedOut, 0)
		timer := time.AfterFunc(a.timeout, func() {
			atomic.StoreInt32(&timedOut, 1)
			_ = res.Body.Close()
		})

		ok := scanner.Scan()

		timer.Stop()

		if ok {
			line := scanner.Text()

			if line == "" {
				if buf.Len() > 0 {
					currentEvent.Data = strings.TrimSpace(buf.String())
					if a.observer != nil {
						a.observer.OnEvent(currentEvent, time.Since(eventStart))
					}
					a.handler(currentEvent)
					buf.Reset()
					eventStart = time.Now()
					currentEvent = Event{ID: a.lastID}
				}
				continue
			}

			if strings.HasPrefix(line, ":") {
				continue
			}

			parts := strings.SplitN(line, ":", 2)
			if len(parts) < 2 {
				continue
			}

			key, partsValue := parts[0], strings.TrimPrefix(parts[1], " ")
			switch key {
			case "data":
				buf.WriteString(partsValue)
				buf.WriteString("\n")
			case "event":
				currentEvent.Name = partsValue
			case "id":
				a.lastID = partsValue
				currentEvent.ID = partsValue
			}
			continue
		}

		if err := scanner.Err(); err != nil {
			if atomic.LoadInt32(&timedOut) == 1 {
				return errors.New("sse stream timed out after " + a.timeout.String())
			}
			return err
		}

		return nil
	}
}
