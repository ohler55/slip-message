// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"strings"
	"sync"
	"time"

	"github.com/ohler55/slip"
)

const defaultMaxMsgs = 1000

type baseQueue struct {
	subjects [][]string
	mu       sync.Mutex
	lastID   int64
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

func (q *baseQueue) subjectMatch(subject []string) bool {
	for _, filter := range q.subjects {
		if subjectMatch(subject, filter) {
			return true
		}
	}
	return false
}

func (q *baseQueue) subjectList() (list slip.List) {
	list = append(list, slip.Symbol("subjects"))
	for _, sa := range q.subjects {
		list = append(list, slip.String(strings.Join(sa, ".")))
	}
	return
}
