// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"sync"
	"time"
)

const defaultMaxMsgs = 1000

type baseQueue struct {
	mu     sync.Mutex
	lastID int64
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
