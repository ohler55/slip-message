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
