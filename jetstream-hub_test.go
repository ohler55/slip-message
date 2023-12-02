// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

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
		Source: `(send jhub :subscribe "top.middle.bottom" (lambda (m) nil))`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribe "top.*.bottom" (lambda (m) nil))`,
		Expect: "/^#<subscriber-flavor [0-9a-f]+>$/",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :subscribe "top.>" (lambda (m) nil))`,
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
		":set-error-handler",
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

func TestJetstreamHubErrorHandler(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(`(send jhub :subscribe "boo.boo" (lambda (m) (panic "broke it")))`).Eval(scope, nil)
	scope.Let("sub", sub)
	scope.Let("error-count", slip.Fixnum(0))
	_ = slip.ReadString(
		`(send jhub :set-error-handler (lambda (err) (setq error-count (1+ error-count))))`).Eval(scope, nil)
	_ = slip.ReadString(`(send jhub :publish "boo.boo" "A message.")`).Eval(scope, nil)
	tt.Equal(t, true, waitForCond(scope, "error-count", slip.Fixnum(1), time.Second*2))
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :set-error-handler)`,
		PanicType: slip.Symbol("error"),
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

func deleteJetstreamStream(t *testing.T, name string) {
	nc, err := nats.Connect(jetstreamURL)
	tt.Nil(t, err)
	js, err2 := nc.JetStream()
	tt.Nil(t, err2)
	_ = js.DeleteStream(name)
	nc.Close()
}

func TestJetstreamHubAddQueue(t *testing.T) {
	deleteJetstreamStream(t, "q2")

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
		Source: `(assoc '(name . "q2") (send jhub :queues))`,
		Expect: `((name . "q2") (retention . :work) (queued . 0) (consumers "name1") (subjects "q2"))`,
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :close-queue "q2")`,
		Expect: "nil",
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :add-queue "q0" :all '("x") :max-messages t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :add-queue "q0" :all '("x") :subjects t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :add-queue "q0" :all '("x") :subjects '(t))`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send jhub :close-queue t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestJetstreamHubWorkQueue(t *testing.T) {
	deleteJetstreamStream(t, "q2")
	scope := slip.NewScope()
	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(`(send jhub :add-queue "q2" :work '("name1")
                               :max-messages 3
                               :subjects '("test.q2"))`).Eval(scope, nil)
	_ = slip.ReadString(`(send jhub :publish "test.q2" "first message")`).Eval(scope, nil)
	_ = slip.ReadString(`(defun condense-jqueue (q)
                          (list
                           (cdr (assoc 'name q))
                           (cdr (assoc 'queued q))))`).Eval(scope, nil)

	sub := slip.ReadString(`(send jhub :subscribe "test.q2" nil :name "name1")`).Eval(scope, nil)
	scope.Let("sub", sub)

	checkCode := `(cdr (assoc 'queued (assoc '(name . "q2") (send jhub :queues))))`
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(1), time.Second*2))

	tf := sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :next sub)`,
		Expect: `/"first message", [0-9]+/`,
	}
	tf.Test(t)
	scope.Let("mv", tf.Result.(slip.Values)[1])
	// Before ack
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(1), time.Second*2))

	_ = slip.ReadString(`(send jhub :publish "test.q2" "second message")`).Eval(scope, nil)
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(2), time.Second*2))

	tf = sliptest.Function{
		Scope:  scope,
		Source: `(send sub :next 0.2)`,
		Expect: `/"second message", [0-9]+/`,
	}
	tf.Test(t)
	scope.Let("mv2", tf.Result.(slip.Values)[1])

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :ack sub mv)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(1), time.Second*2))

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :ack sub mv2)`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(0), time.Second*2))
}

func TestJetstreamHubAllQueue(t *testing.T) {
	deleteJetstreamStream(t, "q2")
	scope := slip.NewScope()
	hub := slip.ReadString(fmt.Sprintf(`(make-instance 'jetstream-hub-flavor :url %q)`, jetstreamURL)).Eval(scope, nil)
	scope.Let("jhub", hub)
	defer func() { _ = slip.ReadString(`(send jhub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(`(send jhub :add-queue "q2" :all '("name1" "name2")
                               :subjects '("test.q2"))`).Eval(scope, nil)
	sub1 := slip.ReadString(`(send jhub :subscribe "test.q2" nil :name "name1")`).Eval(scope, nil)
	scope.Let("sub1", sub1)
	sub2 := slip.ReadString(`(send jhub :subscribe "test.q2" nil :name "name2")`).Eval(scope, nil)
	scope.Let("sub2", sub2)

	_ = slip.ReadString(`(send jhub :publish "test.q2" "first message")`).Eval(scope, nil)
	checkCode := `(cdr (assoc 'queued (assoc '(name . "q2") (send jhub :queues))))`
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(1), time.Second*2))

	// Get next for both subscribers. If the queue was a work queue then the
	// second would fail with a timeout. If both succeed and the queued count
	// is ero then all is working as designed.
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :ack sub1 (nth-value 1 (send sub1 :next)))`,
		Expect: "nil",
	}).Test(t)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send jhub :ack sub2 (nth-value 1 (send sub2 :next)))`,
		Expect: "nil",
	}).Test(t)
	tt.Equal(t, true, waitForCond(scope, checkCode, slip.Fixnum(0), time.Second*2))
}

func waitForCond(s *slip.Scope, code string, target slip.Object, timeout time.Duration) bool {
	start := time.Now()
	for time.Since(start) < timeout {
		value := slip.ReadString(code).Eval(s, nil)
		// fmt.Printf("*** check %s\n", value)
		if value == target {
			return true
		}
		time.Sleep(timeout / 40)
	}
	return false
}
