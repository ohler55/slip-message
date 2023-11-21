// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"time"

	"github.com/ohler55/slip"
)

type queue interface {
	push(msg slip.Object)
	next(consumer string, contentType slip.Object, timeout time.Duration) (msg slip.Object, msgID int64)
	ack(msgID int64)
	asLisp() slip.List
	shutdown()
}
