// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"sync"

	"github.com/ohler55/slip"
)

type baseQueue struct {
	name      string
	consumers []string
	mu        sync.Mutex
	lastID    int64
	retention slip.Symbol
}

func (q *baseQueue) asLisp() (list slip.List) {
	list = make(slip.List, len(q.consumers)+2)
	list[0] = slip.String(q.name)
	list[1] = q.retention
	for i, c := range q.consumers {
		list[i+2] = slip.String(c)
	}
	return
}
