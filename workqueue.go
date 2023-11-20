// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
)

type workQueue struct {
	baseQueue
	// TBD
}

func newWorkQueue(name string, consumers []string) queue {
	return &workQueue{
		baseQueue{
			name:      name,
			consumers: consumers,
			retention: slip.Symbol(":work"),
		},
	}
}

func (q *workQueue) push(msg slip.Object) (msgID int64) {
	// TBD create envelope
	q.mu.Lock()
	// TBD push envelope
	q.lastID++
	msgID = q.lastID
	q.mu.Unlock()

	return
}

func (q *workQueue) next(consumer string) (msg slip.Object, msgID int64) {
	// TBD
	return
}

func (q *workQueue) ack(consumer string, msgID int64) {
	// TBD
	return
}

func (q *workQueue) shutdown() {
	// TBD
}
