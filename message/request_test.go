// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestRequest(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`, scope).Eval(scope, nil)
	scope.Let("hub", hub)
	_ = slip.ReadString(`(message-subscribe hub "one.two" (lambda (m) "reply"))`, scope).Eval(scope, nil)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(message-request hub "one.two" "a message")`,
		Expect: `"reply"`,
	}).Test(t)
}

func TestRequestNotHub(t *testing.T) {
	(&sliptest.Function{
		Source:    `(message-request t "subject")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
