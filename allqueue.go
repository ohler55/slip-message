// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"time"

	"github.com/ohler55/slip"
)

type allQueue struct {
	baseQueue
	// TBD
}

func newAllQueue(name string, consumers []string) queue {
	return &allQueue{
		baseQueue{
			name:      name,
			consumers: consumers,
			retention: slip.Symbol(":all"),
		},
	}
}

func (q *allQueue) asLisp() (list slip.List) {
	q.mu.Lock()
	list = q.baseQueue.asLisp()
	list = append(list, slip.List{slip.Symbol("retention"), slip.Tail{Value: slip.Symbol(":all")}})
	list = append(list, slip.List{slip.Symbol("queued"), slip.Tail{Value: slip.Fixnum(0)}})  // TBD
	list = append(list, slip.List{slip.Symbol("pending"), slip.Tail{Value: slip.Fixnum(0)}}) // TBD
	q.mu.Unlock()

	return
}

func (q *allQueue) push(msg slip.Object) {
	// TBD push onto all chan
	return
}

func (q *allQueue) next(
	consumer string,
	contentType slip.Object,
	timeout time.Duration) (msg slip.Object, msgID int64) {
	// TBD
	return
}

func (q *allQueue) ack(msgID int64) {
	// TBD
	return
}

func (q *allQueue) shutdown() {
	// TBD
}
