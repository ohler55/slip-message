// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"strings"
	"time"

	"github.com/ohler55/slip"
)

type workQueue struct {
	baseQueue
	stack
	consumers []string
}

func newWorkQueue(name string, maxMsgs int, consumers, subjects []string) queue {
	if maxMsgs < 1 {
		maxMsgs = defaultMaxMsgs
	}
	filters := make([][]string, len(subjects))
	for i, sub := range subjects {
		filters[i] = strings.Split(sub, ".")
	}
	return &workQueue{
		baseQueue: baseQueue{
			subjects: filters,
		},
		stack: stack{
			name: name,
			new:  make(chan string, maxMsgs),
		},
		consumers: consumers,
	}
}

func (q *workQueue) qname() string {
	return q.name
}

func (q *workQueue) appendAssoc(list slip.List) slip.List {
	if list == nil {
		list = make(slip.List, 0, 7)
	}
	q.mu.Lock()
	list = append(list, slip.List{slip.Symbol("retention"), slip.Tail{Value: slip.Symbol(":work")}})
	list = q.stack.appendAssoc(list)
	consumers := make(slip.List, len(q.consumers)+1)
	consumers[0] = slip.Symbol("consumers")
	for i, c := range q.consumers {
		consumers[i+1] = slip.String(c)
	}
	list = append(list, consumers)
	list = append(list, q.subjectList())
	q.mu.Unlock()

	return list
}

func (q *workQueue) push(msg slip.Object) {
	q.new <- string(msg.(slip.String))
}

func (q *workQueue) next(
	consumer string,
	contentType slip.Object,
	timeout time.Duration) (msg slip.Object, msgID int64) {

	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	select {
	case smsg := <-q.new:
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

func (q *workQueue) ack(_ string, msgID int64) {
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
	close(q.new)
}
