// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestUnsubscribe(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`).Eval(scope, nil)
	scope.Let("hub", hub)
	_ = slip.ReadString(`(message-subscribe hub "one.two" (lambda (m) nil))`).Eval(scope, nil)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(message-unsubscribe hub "one.two")`,
		Expect: "1",
	}).Test(t)
}

func TestUnsubscribeNotHub(t *testing.T) {
	(&sliptest.Function{
		Source:    `(message-unsubscribe t "subject")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
