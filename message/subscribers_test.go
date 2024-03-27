// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestSubscribers(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`).Eval(scope, nil)
	scope.Let("hub", hub)
	_ = slip.ReadString(`(message-subscribe hub "one.two" (lambda (m) nil))`).Eval(scope, nil)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(message-subscribers hub "one.two")`,
		Expect: `/\(#<subscriber-flavor [0-9a-f]+>\)/`,
	}).Test(t)
}

func TestSubscribersNotHub(t *testing.T) {
	(&sliptest.Function{
		Source:    `(message-subscribers t "subject")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
