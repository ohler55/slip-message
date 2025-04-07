// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestSubscribe(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`, scope).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(message-subscribe hub "one.two" (lambda (m) nil))`,
		Expect: "/#<subscriber-flavor [0-9a-f]+>/",
	}).Test(t)
}

func TestSubscribeNotHub(t *testing.T) {
	(&sliptest.Function{
		Source:    `(message-subscribe t "subject" nil)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
