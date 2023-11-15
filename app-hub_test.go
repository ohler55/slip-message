// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/gi"
	"github.com/ohler55/slip/sliptest"
)

func TestAppHubSubscribe(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `hub`,
		Expect: "/^#<app-hub-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribe "top.middle.bottom" nil)`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribe "top.*.bottom" nil)`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribe "top.>" nil)`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers)`,
		Expect: `/^\(#<subscriber-flavor [0-9a-f]+> #<subscriber-flavor [0-9a-f]+> #<subscriber-flavor [0-9a-f]+>\)$/`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers "top.something.bottom")`,
		Expect: `/^\(#<subscriber-flavor [0-9a-f]+> #<subscriber-flavor [0-9a-f]+>\)$/`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :unsubscribe (nth 2 (send hub :subscribers)))`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers "top.middle")`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers "top.middle.bottom.basement")`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :unsubscribe "top.middle.bottom")`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers)`,
		Expect: `/^\(#<subscriber-flavor [0-9a-f]+>\)$/`,
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :unsubscribe (car (send hub :subscribers)))`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :subscribers)`,
		Expect: "nil",
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :subscribe 7 nil)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :subscribers 7)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :subscribe "subject" :bad nil)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestReaderDocs(t *testing.T) {
	scope := slip.NewScope()
	var out strings.Builder
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	for _, method := range []string{
		":init",
		":subscribe",
		":unsubscribe",
		":subscribers",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method app-hub-flavor %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestAppHubPublish(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	scope.Set("mq", mq)

	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	sub := slip.ReadString(
		`(send hub :subscribe "top.middle.*" (lambda (m) (format t "~A~%" m) (channel-push mq m)))`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" "A message.")`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.Equal(t, `"A message."`, slip.ObjectString(m))
}
