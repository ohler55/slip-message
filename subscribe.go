// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := Subscribe{Function: slip.Function{Name: "message-subscribe", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "message-subscribe",
			Args: []*slip.DocArg{
				{
					Name: "hub",
					Type: "instance",
					Text: "The message hub to send the _:subscribe_ method.",
				},
				{
					Name: "subject",
					Type: "string",
					Text: "Subject to listen on.",
				},
				{
					Name: "callback",
					Type: "function",
					Text: `Can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.`,
				},
				{Name: "&key"},
				{
					Name: "content-type",
					Type: "keyword",
					Text: `Content-type is an optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.`,
				},
				{
					Name: "name",
					Type: "string",
					Text: "Name of the subscriber is used with work queues.",
				},
			},
			Return: "instance",
			Text: `__message-subscribe__ returns a _subscriber-flavor_ instance that represents
a subscription on the _subject_.`,
			Examples: []string{
				`(setq hub (make-app-hub)) => #<app-hub-flavor 1234>`,
				`(message-subscribe hub) => #<subscriber-flavor 1234>`,
			},
		}, &Pkg)
}

// Subscribe represents the message-hub-close function.
type Subscribe struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *Subscribe) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.ArgCountCheck(f, args, 1, -1)
	inst, ok := args[0].(*flavors.Instance)
	if !ok {
		slip.PanicType("hub", args[0], "instance")
	}
	return inst.Receive(s, ":subscribe", args[1:], depth)
}
