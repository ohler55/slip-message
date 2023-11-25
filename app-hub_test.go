// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ohler55/ojg/tt"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
	"github.com/ohler55/slip/pkg/gi"
	"github.com/ohler55/slip/sliptest"
)

func TestAppHubSubscribe(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
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
		":publish",
		":request",
		":close",
		":add-queue",
		":close-queue",
		":queues",
		":next",
		":ack",
	} {
		_ = slip.ReadString(fmt.Sprintf(`(describe-method app-hub-flavor %s out)`, method)).Eval(scope, nil)
		tt.Equal(t, true, strings.Contains(out.String(), method))
		out.Reset()
	}
}

func TestAppHubPublish(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	defer close(mq)
	scope.Set("mq", mq)

	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(
		`(send hub :subscribe "top.middle.*" (lambda (m) (channel-push mq m)))`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" "A message.")`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.Equal(t, `"A message."`, slip.ObjectString(m))
}

func TestAppHubPublishLisp(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	defer close(mq)
	scope.Set("mq", mq)

	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(
		`(send hub :subscribe "top.middle.*" (lambda (m) (channel-push mq m)))`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" '(a 2 c 4))`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.SameType(t, slip.List{}, m)
	tt.Equal(t, "(a 2 c 4)", m.String())
}

func TestAppHubPublishLisp2(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	defer close(mq)
	scope.Set("mq", mq)

	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(
		`(send hub :subscribe "top.middle.*" (lambda (m) (channel-push mq m)) :content-type :lisp)`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" '(a 2 c 4))`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.SameType(t, slip.List{}, m)
	tt.Equal(t, "(a 2 c 4)", m.String())
}

func TestAppHubPublishJSON(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	defer close(mq)
	scope.Set("mq", mq)

	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(
		`(send hub :subscribe "top.middle.*" (lambda (m) (channel-push mq m)))`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" (make-instance 'bag-flavor :parse "{a:1}"))`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.SameType(t, &flavors.Instance{}, m)
	tt.Equal(t, "/#<bag-flavor [0-9a-f]+>/", m.String())
}

func TestAppHubPublishJSON2(t *testing.T) {
	scope := slip.NewScope()
	mq := make(gi.Channel, 10)
	defer close(mq)
	scope.Set("mq", mq)

	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	sub := slip.ReadString(
		`(send hub :subscribe "top.middle.*" (lambda (m) (channel-push mq m)) :content-type :json)`).Eval(scope, nil)
	scope.Let("sub", sub)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" (make-instance 'bag-flavor :parse "{a:1}"))`,
		Expect: "nil",
	}).Test(t)
	m := <-mq
	tt.SameType(t, &flavors.Instance{}, m)
	tt.Equal(t, "/#<bag-flavor [0-9a-f]+>/", m.String())

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :publish "top.middle.bottom" (make-instance 'bag-flavor :parse "{a:1}") :sen)`,
		Expect: "nil",
	}).Test(t)
	m = <-mq
	tt.SameType(t, &flavors.Instance{}, m)
	tt.Equal(t, "/#<bag-flavor [0-9a-f]+>/", m.String())
}

func TestAppHubPublishBad(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :publish "a.b")`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :publish 7 "xyz")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :publish "a.b" (make-instance 'vanilla-flavor))`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestAppHubRequest(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(
		`(send hub :subscribe "requests" (lambda (m) "got it!"))`).Eval(scope, nil)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :request "requests" "A message.")`,
		Expect: `"got it!"`,
	}).Test(t)
}

func TestAppHubRequestTimeout(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(
		`(send hub :subscribe "requests" (lambda (m) (sleep 0.1) "got it!"))`).Eval(scope, nil)
	(&sliptest.Function{
		Scope: scope,
		Source: `(send hub :request "requests"
                                       (make-instance 'bag-flavor :parse "{}")
                                       :timeout 0.01 :content-type :sen)`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
}

func TestAppHubRequestMultiReply(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(
		`(send hub :subscribe "requests" (lambda (m) "got it in one!"))`).Eval(scope, nil)
	_ = slip.ReadString(
		`(send hub :subscribe "requests" (lambda (m) "got it in two!"))`).Eval(scope, nil)
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :request "requests" "A message.")`,
		Expect: `/"got it in .*!"/`,
	}).Test(t)
	// Wait for the slower subscriber to recover.
	time.Sleep(time.Millisecond * 10)
}

func TestAppHubRequestBadArg(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :request "requests")`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :request t "message")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :request "bad" "message" :timeout t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :request "bad" "message" :bad t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestAppHubQueue(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :add-queue "q2" :work '("name1" "name2"))`,
		Expect: "nil",
	}).Test(t)
	queues := slip.ReadString(`(send hub :queues)`).Eval(scope, nil).(slip.List)
	tt.Equal(t, `(((retention . :work) (name . "q2") (queued . 0) (pending . 0) (acked . 0) `+
		`(average-ack . 0) (consumers "name1" "name2")))`, slip.ObjectString(queues))

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :add-queue "q3" :all '("name3" "name4"))`,
		Expect: "nil",
	}).Test(t)
	queues = slip.ReadString(`(send hub :queues)`).Eval(scope, nil).(slip.List)
	tt.Equal(t, `((retention . :work) (name . "q2") (queued . 0) (pending . 0) (acked . 0) `+
		`(average-ack . 0) (consumers "name1" "name2"))`, slip.ObjectString(queues[0]))

	tt.Equal(t, `((retention . :all) (name . "q3")
                  (consumers ((name . "name3") (queued . 0) (pending . 0) (acked . 0) (average-ack . 0))
                             ((name . "name4") (queued . 0) (pending . 0) (acked . 0) (average-ack . 0))))`,
		slip.ObjectString(queues[1]))

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :close-queue "q2")`,
		Expect: "nil",
	}).Test(t)
	queues = slip.ReadString(`(send hub :queues)`).Eval(scope, nil).(slip.List)
	tt.Equal(t, `(((retention . :all) (name . "q3")
                   (consumers ((name . "name3") (queued . 0) (pending . 0) (acked . 0) (average-ack . 0))
                              ((name . "name4") (queued . 0) (pending . 0) (acked . 0) (average-ack . 0)))))`,
		slip.ObjectString(queues))
}

func TestAppHubAddQueuePanics(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue "q2" :work)`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue t :work '("name1"))`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue "q2" :bad '("name1"))`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue "q2" :bad '("name1"))`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue "q2" :work '(t))`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue "q2" :work t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :add-queue "q2" :work '("x") t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestAppHubCloseQueuePanics(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :close-queue t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}

func TestAppHubWorkQueue(t *testing.T) {
	scope := slip.NewScope()
	hub := slip.ReadString(`(make-instance 'app-hub-flavor)`).Eval(scope, nil)
	scope.Let("hub", hub)
	defer func() { _ = slip.ReadString(`(send hub :close)`).Eval(scope, nil) }()
	_ = slip.ReadString(`(send hub :add-queue "q2" :work '("name1" "name2") 3)`).Eval(scope, nil)
	_ = slip.ReadString(`(send hub :publish "q2" "first message")`).Eval(scope, nil)
	_ = slip.ReadString(`(defun condense-queue (q)
                          (list
                           (cdr (assoc 'name q))
                           (cdr (assoc 'queued q))
                           (cdr (assoc 'pending q))
                           (cdr (assoc 'acked q))))`).Eval(scope, nil)

	sub := slip.ReadString(`(send hub :subscribe "q2" nil :name "name1")`).Eval(scope, nil)
	scope.Let("sub", sub)

	tf := sliptest.Function{
		Scope:  scope,
		Source: `(send hub :next sub)`,
		Expect: `/"first message", [0-9]+/`,
	}
	tf.Test(t)
	scope.Let("mv", tf.Result.(slip.Values)[1])
	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :next sub)`,
		Expect: "nil",
	}).Test(t)
	_ = slip.ReadString(`(send hub :publish "q2" "second message")`).Eval(scope, nil)
	queue := slip.ReadString(`(condense-queue (car (send hub :queues)))`).Eval(scope, nil)
	tt.Equal(t, `("q2" 1 1 0)`, slip.ObjectString(queue))

	tf = sliptest.Function{
		Scope:  scope,
		Source: `(send sub :next 0.2)`,
		Expect: `/"second message", [0-9]+/`,
	}
	tf.Test(t)
	scope.Let("mv2", tf.Result.(slip.Values)[1])
	queue = slip.ReadString(`(condense-queue (car (send hub :queues)))`).Eval(scope, nil)
	tt.Equal(t, `("q2" 0 2 0)`, slip.ObjectString(queue))

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :ack sub mv)`,
		Expect: "nil",
	}).Test(t)
	queue = slip.ReadString(`(condense-queue (car (send hub :queues)))`).Eval(scope, nil)
	tt.Equal(t, `("q2" 0 1 1)`, slip.ObjectString(queue))

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send hub :ack sub mv2)`,
		Expect: "nil",
	}).Test(t)
	queue = slip.ReadString(`(condense-queue (car (send hub :queues)))`).Eval(scope, nil)
	tt.Equal(t, `("q2" 0 0 2)`, slip.ObjectString(queue))

	(&sliptest.Function{
		Scope:  scope,
		Source: `(send sub :next)`,
		Expect: "nil",
	}).Test(t)

	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :next)`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :next t "q2")`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :next sub :timeout t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :next sub :bad t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :ack)`,
		PanicType: slip.Symbol("error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :ack sub t)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
	(&sliptest.Function{
		Scope:     scope,
		Source:    `(send hub :ack (make-instance 'vanilla-flavor) 123)`,
		PanicType: slip.Symbol("type-error"),
	}).Test(t)
}
