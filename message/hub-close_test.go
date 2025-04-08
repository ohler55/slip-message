// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestHubClose(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`, scope).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:  scope,
		Source: "(message-hub-close hub)",
		Expect: "nil",
	}).Test(t)
}

func TestHubCloseNotHub(t *testing.T) {
	(&sliptest.Function{
		Source:    "(message-hub-close t)",
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
