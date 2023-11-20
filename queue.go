// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import "github.com/ohler55/slip"

type queue interface {
	push(msg slip.Object) (msgID int64)
	next(consumer string) (msg slip.Object, msgID int64)
	ack(consumer string, msgID int64)
	asLisp() slip.List
	shutdown()
}
