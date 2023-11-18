// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestPublish(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(message-publish hub "one.two" "a message")`,
		Expect: "nil",
	}).Test(t)
}

func TestPublishNotHub(t *testing.T) {
	(&sliptest.Function{
		Source:    `(message-publish t "subject")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
