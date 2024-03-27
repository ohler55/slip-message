// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/sliptest"
)

func TestSubscriberBasic(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	sub := slip.ReadString(
		`(send hub :subscribe "one.two.three" (lambda (m) nil) :content-type :json :name "tester")`).Eval(scope, nil)
	scope.Let("sub", sub)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :hub)`,
		Expect: `/#<app-hub-flavor [0-9a-f]+>/`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :subject)`,
		Expect: `"one.two.three"`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :callback)`,
		Expect: `/#<function \(lambda \(m\)\) \{[0-9a-f]+\}>/`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :name)`,
		Expect: `"tester"`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :content-type)`,
		Expect: ":json",
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :set-content-type :lisp)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :content-type)`,
		Expect: ":lisp",
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :set-callback nil)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :callback)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :set-callback (lambda (m) nil))`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :callback)`,
		Expect: `/#<function \(lambda \(m\)\) \{[0-9a-f]+\}>/`,
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :close)`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers)`,
		Expect: "nil",
	}).Test(t)
}

func TestSubscriberDocs(t *testing.T) {
	scope := slip.NewScope()
	var out strings.Builder
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	for _, method := range []string{
		":hub",
		":subject",
		":callback",
		":name",
		":content-type",
		":set-callback",
		":set-content-type",
		":close",
		":next",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method subscriber-flavor %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestSubscriberBadName(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :subscribe "one.two.three" nil :name 7)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestSubscriberBadSubject(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :subscribe 7 nil)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestSubscriberBadContentType(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :subscribe "one.two.three" nil :content-type 7)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
