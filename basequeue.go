// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"sync"
	"time"

	"github.com/ohler55/slip"
)

type baseQueue struct {
	name      string
	consumers []string
	mu        sync.Mutex
	lastID    int64
	retention slip.Symbol
	cnt       int64
	tsum      int64
}

// Must be mu.Locked first.
func (q *baseQueue) asLisp() (list slip.List) {
	list = make(slip.List, 4)
	list[0] = slip.List{slip.Symbol("name"), slip.Tail{Value: slip.String(q.name)}}
	list[1] = slip.List{slip.Symbol("acked"), slip.Tail{Value: slip.Fixnum(q.cnt)}}
	list[2] = slip.List{slip.Symbol("average-ack"), slip.Tail{Value: slip.Fixnum(q.averageAck())}}
	consumers := make(slip.List, len(q.consumers)+1)
	consumers[0] = slip.Symbol("consumers")
	for i, c := range q.consumers {
		consumers[i+1] = slip.String(c)
	}
	list[3] = consumers

	return
}

// Must be mu.Locked first.
func (q *baseQueue) nextID() (id int64) {
	id = time.Now().UnixNano()
	if id <= q.lastID {
		id = q.lastID + 1
	}
	q.lastID = id
	return
}

func (q *baseQueue) averageAck() (average time.Duration) {
	if 0 < q.cnt {
		average = time.Duration(q.tsum / q.cnt)
	}
	return
}
