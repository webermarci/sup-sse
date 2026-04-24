# sup-sse

[![Go Reference](https://pkg.go.dev/badge/github.com/webermarci/sup-sse.svg)](https://pkg.go.dev/github.com/webermarci/sup-sse)
[![Test](https://github.com/webermarci/sup-sse/actions/workflows/test.yml/badge.svg)](https://github.com/webermarci/sup-sse/actions/workflows/test.yml)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

`sup-sse` is a high-reliability Server-Sent Events (SSE) client implementation for Go, built on top of the [sup](https://github.com/webermarci/sup) actor library. It provides thread-safe, supervised, and observable way to interact with Server-Sent Events (SSE) streams.

## Features

* **Actor-Based Concurrency**: Thread-safe access to hardware. Multiple goroutines can call the actor safely; the actor handles the queue.
* **Supervised Lifecycle**: Designed to run under a `sup.Supervisor`. If the connection drops, the actor returns a fatal error, allowing the supervisor to handle reconnection.
* **Last-Event-ID**: Automatically tracks the last event ID and includes it in the `Last-Event-ID` header for reconnections.
* **Rich Observability**: Built-in `Observer` interface to monitor events and errors.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "time"

		"github.com/webermarci/sup"
    sse "github.com/webermarci/sup-sse"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	handler := func(sse.Event event) {
		fmt.Println(event)
	}
	
	client := sse.NewSSEActor("http://localhost:8080", handler,
		sse.WithTimeout(10 * time.Second),
	)

	supervisor := sup.NewSupervisor(
		sup.WithActor(client),
		sup.WithPolicy(sup.Permanent),
		sup.WithRestartDelay(time.Second),
		sup.WithRestartLimit(5, 10 * time.Second),
	)
	
	supervisor.Run(ctx)
	supervisor.Wait()
}
```
