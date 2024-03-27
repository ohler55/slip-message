// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"testing"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestMakeAppHub(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-app-hub)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	(&sliptest.Function{
		Scope:  scope,
		Source: `hub`,
		Expect: "/^#<app-hub-flavor [0-9a-f]+>$/",
	}).Test(t)
}
