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

func (q *workQueue) asLisp() (list slip.List) {
	q.mu.Lock()
	list = q.baseQueue.asLisp()
	list = append(list, slip.List{slip.Symbol("retention"), slip.Tail{Value: slip.Symbol(":work")}})
	list = append(list, slip.List{slip.Symbol("queued"), slip.Tail{Value: slip.Fixnum(len(q.stack))}})
	var pending slip.Fixnum
	for _, t := range q.pending {
		if t != 0 {
			pending++
		}
	}
	list = append(list, slip.List{slip.Symbol("pending"), slip.Tail{Value: pending}})
	q.mu.Unlock()

	return
}

func (q *workQueue) push(msg slip.Object) {
	q.stack <- string(msg.(slip.String))
}

func (q *workQueue) next(
	consumer string,
	contentType slip.Object,
	timeout time.Duration) (msg slip.Object, msgID int64) {

	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	select {
	case smsg := <-q.stack:
		msg = decodeMessage(slip.String(smsg), contentType)
		q.mu.Lock()
		q.lastID++
		msgID = q.nextID()
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
			q.tsum += time.Now().UnixNano() - mid
			q.cnt++
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
