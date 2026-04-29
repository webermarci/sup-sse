package sse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSSEActor_Parser(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "id: 101\ndata: hello\n\n")
	}))
	defer server.Close()

	done := make(chan struct{})
	var received Event
	handler := func(e Event) {
		received = e
		close(done)
	}

	actor := NewActor(t.Name(), server.URL, handler, WithHTTPClient(&http.Client{}))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go actor.Run(ctx)

	select {
	case <-done:
		if received.ID != "101" || received.Data != "hello" {
			t.Errorf("got %+v", received)
		}
	case <-ctx.Done():
		t.Fatal("Parser test timed out")
	}
}

func TestSSEActor_WatchdogTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	actor := NewActor(t.Name(), server.URL, func(e Event) {},
		WithTimeout(100*time.Millisecond),
		WithHTTPClient(&http.Client{}),
	)

	err := actor.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected timeout, got: %v", err)
	}
}

func TestSSEActor_LastEventID_Persistence(t *testing.T) {
	var mu sync.Mutex
	requestCount := 0
	capturedID := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		capturedID = r.Header.Get("Last-Event-ID")
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		if requestCount == 1 {
			io.WriteString(w, "id: msg_99\ndata: hi\n\n")
			return
		}
	}))
	defer server.Close()

	actor := NewActor(t.Name(), server.URL, func(e Event) {}, WithHTTPClient(&http.Client{}))

	ctx1, cancel1 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel1()
	_ = actor.Run(ctx1)

	if actor.lastID != "msg_99" {
		t.Fatalf("Expected lastID msg_99, got %s", actor.lastID)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	_ = actor.Run(ctx2)

	mu.Lock()
	finalID := capturedID
	mu.Unlock()

	if finalID != "msg_99" {
		t.Errorf("Expected header Last-Event-ID: msg_99, got %s", finalID)
	}
}
