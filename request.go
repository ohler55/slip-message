// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := Request{Function: slip.Function{Name: "message-request", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "message-request",
			Args: []*slip.DocArg{
				{
					Name: "hub",
					Type: "instance",
					Text: "The message hub to send the _:request_ method.",
				},
				{
					Name: "subject",
					Type: "string",
					Text: "Subject to request on.",
				},
				{
					Name: "message",
					Type: "object",
					Text: `either a _string_ for :raw content, a _bag_ for JSON or SEN format,
or an sexpression for _lisp_ content.`,
				},
				{Name: "&key"},
				{
					Name: "content-type",
					Type: "keyword",
					Text: `Content-type of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.`,
				},
				{
					Name: "timeout",
					Type: "real",
					Text: `a real number denoting the seconds to wait for a reply before a timeout panic.`,
				},
			},
			Return: "nil",
			Text:   `__message-request__ send a request message on _subject_ and wait for a reply.`,
			Examples: []string{
				`(setq hub (make-app-hub)) => #<app-hub-flavor 1234>`,
				`(message-subscribe hub "subject" (lambda (m) "reply"))`,
				`(message-request hub "subject" "a message") => "reply"`,
			},
		}, &Pkg)
}

// Request represents the message-hub-close function.
type Request struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *Request) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.ArgCountCheck(f, args, 1, -1)
	inst, ok := args[0].(*flavors.Instance)
	if !ok {
		slip.PanicType("hub", args[0], "instance")
	}
	return inst.Receive(s, ":request", args[1:], depth)
}
