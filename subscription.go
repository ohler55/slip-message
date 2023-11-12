// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

type subscription struct {
	hub         *flavors.Instance
	subject     string
	contentType slip.Object
	callback    slip.Caller
	self        *flavors.Instance
}

func (sub *subscription) setContentType(ct slip.Object) {
	switch ct {
	case nil, slip.Symbol(":json"), slip.Symbol(":lisp"), slip.Symbol(":auto"), slip.Symbol(":raw"):
		sub.contentType = ct
	default:
		slip.PanicType(":content-type", ct, "nil", ":json", ":lisp", ":auto", ":raw")
	}
}
