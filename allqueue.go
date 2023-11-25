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
	list = append(list, slip.List{slip.Symbol("retention"), slip.Tail{Value: slip.Symbol(":all")}})
	list = append(list, slip.List{slip.Symbol("name"), slip.Tail{Value: slip.String(q.name)}})
	q.mu.Lock()
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
	q.mu.Unlock()
	list = append(list, consumers)

	return list
}

func (q *allQueue) push(msg slip.Object) {
	for _, st := range q.consumers {
		st.new <- string(msg.(slip.String))
	}
	return
}

func (q *allQueue) next(
	consumer string,
	contentType slip.Object,
	timeout time.Duration) (msg slip.Object, msgID int64) {

	if timeout <= 0 {
		timeout = time.Nanosecond
	}
	if st := q.consumers[consumer]; st != nil {
		select {
		case smsg := <-st.new:
			msg = decodeMessage(slip.String(smsg), contentType)
			q.mu.Lock()
			q.lastID++
			msgID = q.nextID()
			st.pending = append(st.pending, msgID)
			q.mu.Unlock()
		case <-time.After(timeout):
			// leave msg as nil and id as 0
		}
	}
	return
}

func (q *allQueue) ack(consumer string, msgID int64) {
	if st := q.consumers[consumer]; st != nil {
		q.mu.Lock()
		for i, mid := range st.pending {
			if msgID == mid {
				st.tsum += time.Now().UnixNano() - mid
				st.cnt++
				st.pending[i] = 0
				if i == 0 {
					for 0 < len(st.pending) && st.pending[0] == 0 {
						st.pending = st.pending[1:]
					}
				}
				break
			}
		}
		q.mu.Unlock()
	}
	return
}

func (q *allQueue) shutdown() {
	for _, st := range q.consumers {
		close(st.new)
	}
}
