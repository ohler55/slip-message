// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"sort"
	"time"

	"github.com/ohler55/slip"
)

type allQueue struct {
	baseQueue
	name      string
	consumers map[string]*stack
}

func newAllQueue(name string, maxMsgs int, consumers []string) queue {
	if maxMsgs < 1 {
		maxMsgs = defaultMaxMsgs
	}
	q := allQueue{
		name:      name,
		consumers: map[string]*stack{},
	}
	for _, cn := range consumers {
		q.consumers[cn] = &stack{
			name: cn,
			new:  make(chan string, maxMsgs),
		}
	}
	return &q
}

func (q *allQueue) appendAssoc(list slip.List) slip.List {
	if list == nil {
		list = make(slip.List, 0, 6)
	}
	q.mu.Lock()
	list = append(list, slip.List{slip.Symbol("retention"), slip.Tail{Value: slip.Symbol(":all")}})
	list = append(list, slip.List{slip.Symbol("name"), slip.Tail{Value: slip.String(q.name)}})
	consumers := make(slip.List, 0, len(q.consumers)+1)
	consumers = append(consumers, slip.Symbol("consumers"))
	cnames := make([]string, 0, len(q.consumers))
	for cn := range q.consumers {
		cnames = append(cnames, cn)
	}
	sort.Strings(cnames)
	for _, cn := range cnames {
		consumers = append(consumers, q.consumers[cn].appendAssoc(nil))
	}
	list = append(list, consumers)
	q.mu.Unlock()

	return list
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

func (q *allQueue) ack(consumer string, msgID int64) {
	// TBD
	return
}

func (q *allQueue) shutdown() {
	for _, st := range q.consumers {
		close(st.new)
	}
}
