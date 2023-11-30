// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/gi"
	"github.com/ohler55/slip/sliptest"
)

func TestJetstreamHubSubscribe(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	(&sliptest.Function{
		Scope:  scope,
		Source: `jhub`,
		Expect: "/^#<jetstream-hub-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribe "top.middle.bottom" nil)`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribe "top.*.bottom" nil)`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribe "top.>" nil)`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribers)`,
		Expect: `/^\(#<subscriber-flavor [0-9a-f]+> #<subscriber-flavor [0-9a-f]+> #<subscriber-flavor [0-9a-f]+>\)$/`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribers "top.something.bottom")`,
		Expect: `/^\(#<subscriber-flavor [0-9a-f]+> #<subscriber-flavor [0-9a-f]+>\)$/`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :unsubscribe (nth 2 (send jhub :subscribers)))`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribers "top.middle")`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribers "top.middle.bottom.basement")`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :unsubscribe "top.middle.bottom")`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribers)`,
		Expect: `/^\(#<subscriber-flavor [0-9a-f]+>\)$/`,
	}).Test(t)

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :unsubscribe (car (send jhub :subscribers)))`,
		Expect: "1",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribers)`,
		Expect: "nil",
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :subscribe 7 nil)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :subscribers 7)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :subscribe "subject" :bad nil)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestJetstreamHubNewErrors(t *testing.T) {
	(&sliptest.Function{
		Source:    `(make-instance 'jetstream-hub-flavor :url t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Source:    `(make-instance 'jetstream-hub-flavor :url "localhost:0")`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
}

func TestJetstreamHubDocs(t *testing.T) {
	scope := slip.NewScope()
	var out strings.Builder
	scope.Let(slip.Symbol("out"), &slip.OutputStream{Writer: &out})

	for _, method := range []string{
		":init",
		":subscribe",
		":unsubscribe",
		":subscribers",
		":publish",
		":request",
		":close",
		":add-queue",
		":close-queue",
		":queues",
		":next",
		":ack",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method jetstream-hub-flavor %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestJetstreamHubPublish(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	defer close(mq)
	scope.Set("mq", mq)

	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(
		`(send jhub :subscribe "top.middle.*" (lambda (m) (channel-push mq m)))`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :publish "top.middle.bottom" "A message.")`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.Equal(t, `"A message."`, slip.ObjectString(m))

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :publish "nothing")`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :publish t "nothing")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestJetstreamHubRequest(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(
		`(send jhub :subscribe "requests" (lambda (m) "got it!"))`).Eval(scope, nil)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :request "requests" "A message.")`,
		Expect: `"got it!"`,
	}).Test(t)
}

func TestJetstreamHubAddQueue(t *testing.T) {
	nc, err := nats.Connect(jetstreamURL)
	tt.Nil(t, err)
	js, err := nc.JetStream()
	tt.Nil(t, err)

	_ = js.DeleteStream("q2")

	scope := slip.NewScope()
	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :add-queue "q2" :work '("name1"))`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :queues)`,
		Expect: `((name . "q2") (retention . :work) (queued . 0) (consumers "name1"))`,
	}).Test(t)
}
