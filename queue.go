// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"time"

	"github.com/ohler55/slip"
)

type queue interface {
	push(msg slip.Object)
	next(consumer string, contentType slip.Object, timeout time.Duration) (msg slip.Object, msgID int64)
	ack(consumer string, msgID int64)
	appendAssoc(list slip.List) slip.List
	shutdown()
}
