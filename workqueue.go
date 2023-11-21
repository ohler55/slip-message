// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"time"

	"github.com/ohler55/slip"
)

const defaultMaxMsgs = 1000

type workQueue struct {
	baseQueue
	stack   chan string
	pending []int64
}

func newWorkQueue(name string, maxMsgs int, consumers []string) queue {
	if maxMsgs < 1 {
		maxMsgs = defaultMaxMsgs
	}
	return &workQueue{
		baseQueue: baseQueue{
			name:      name,
			consumers: consumers,
			retention: slip.Symbol(":work"),
		},
		stack: make(chan string, maxMsgs),
	}
}

func (q *workQueue) push(msg slip.Object) {
	q.stack <- string(msg.(slip.String))
}

func (q *workQueue) next(
	consumer string,
	contentType slip.Object,
	timeout time.Duration) (msg slip.Object, msgID int64) {

	var found bool
	for _, c := range q.consumers {
		if c == consumer {
			found = true
			break
		}
	}
	if !found {
		slip.NewPanic("%s is not a consumer on queue %s", consumer, q.name)
	}
	select {
	case smsg := <-q.stack:
		msg = decodeMessage(slip.String(smsg), contentType)
		q.mu.Lock()
		q.lastID++
		msgID = q.lastID
		q.pending = append(q.pending, msgID)
		q.mu.Unlock()
	case <-time.After(timeout):
		// leave msg as nil and id as 0
	}
	return
}

func (q *workQueue) ack(msgID int64) {
	q.mu.Lock()
	for i, mid := range q.pending {
		if msgID == mid {
			q.pending[i] = 0
			if i == 0 {
				for 0 < len(q.pending) && q.pending[0] == 0 {
					q.pending = q.pending[1:]
				}
			}
			break
		}
	}
	q.mu.Unlock()
}

func (q *workQueue) shutdown() {
	close(q.stack)
}
