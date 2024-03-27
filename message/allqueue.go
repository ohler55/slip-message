// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"sort"
	"strings"
	"time"

	"github.com/ohler55/slip"
)

type allQueue struct {
	baseQueue
	name      string
	consumers map[string]*stack
}

func newAllQueue(name string, maxMsgs int, consumers, subjects []string) queue {
	if maxMsgs < 1 {
		maxMsgs = defaultMaxMsgs
	}
	filters := make([][]string, len(subjects))
	for i, sub := range subjects {
		filters[i] = strings.Split(sub, ".")
	}
	q := allQueue{
		baseQueue: baseQueue{
			subjects: filters,
		},
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

func (q *allQueue) qname() string {
	return q.name
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
	list = append(list, consumers)
	list = append(list, q.subjectList())
	q.mu.Unlock()

	return list
}

func (q *allQueue) push(msg slip.Object) {
	for _, st := range q.consumers {
		st.new <- string(msg.(slip.String))
	}
	return
}

func (q *allQueue) nextMsg(st *stack, smsg string, contentType slip.Object) (msg slip.Object, msgID int64) {
	msg = decodeMessage(slip.String(smsg), contentType)
	q.mu.Lock()
	q.lastID++
	msgID = q.nextID()
	st.pending = append(st.pending, msgID)
	q.mu.Unlock()

	return
}

func (q *allQueue) next(
	consumer string,
	contentType slip.Object,
	timeout time.Duration) (msg slip.Object, msgID int64) {

	if st := q.consumers[consumer]; st != nil {
		if 0 < len(st.new) {
			return q.nextMsg(st, <-st.new, contentType)
		}
		if timeout <= 0 {
			timeout = time.Nanosecond
		}
		select {
		case smsg := <-st.new:
			return q.nextMsg(st, smsg, contentType)
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
