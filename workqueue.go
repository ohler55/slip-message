// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
)

type workQueue struct {
	baseQueue
	stack []*workEnv
}

func newWorkQueue(name string, consumers []string) queue {
	return &workQueue{
		baseQueue: baseQueue{
			name:      name,
			consumers: consumers,
			retention: slip.Symbol(":work"),
		},
	}
}

func (q *workQueue) push(msg slip.Object) (msgID int64) {
	env := workEnv{msg: string(msg.(slip.String))}
	q.mu.Lock()
	q.lastID++
	msgID = q.lastID
	env.mid = msgID
	q.stack = append(q.stack, &env)
	q.mu.Unlock()

	return
}

func (q *workQueue) next(consumer string, contentType slip.Object) (msg slip.Object, msgID int64) {
	var found bool
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, c := range q.consumers {
		if c == consumer {
			found = true
			break
		}
	}
	if !found {
		slip.NewPanic("%s is not a consumer on queue %s", consumer, q.name)
	}
	for _, env := range q.stack {
		if env.status == newStatus {
			msgID = env.mid
			msg = decodeMessage(slip.String(env.msg), contentType)
			break
		}
	}
	return
}

func (q *workQueue) ack(msgID int64) {
	q.mu.Lock()
	for i, env := range q.stack {
		if msgID == env.mid {
			if i == 0 {
				for 0 < len(q.stack) && q.stack[0].status == ackedStatus {
					q.stack = q.stack[1:]
				}
			}
			break
		}
	}
	q.mu.Unlock()
}

func (q *workQueue) shutdown() {
}
