// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
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

func (q *allQueue) push(msg slip.Object) (msgID int64) {
	// TBD create envelope
	q.mu.Lock()
	// TBD push envelope
	q.lastID++
	msgID = q.lastID
	q.mu.Unlock()

	return
}

func (q *allQueue) next(consumer string, contentType slip.Object) (msg slip.Object, msgID int64) {
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
