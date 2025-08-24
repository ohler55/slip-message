// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := Publish{Function: slip.Function{Name: "message-publish", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "message-publish",
			Args: []*slip.DocArg{
				{
					Name: "hub",
					Type: "instance",
					Text: "The message hub to send the _:publish_ method.",
				},
				{
					Name: "subject",
					Type: "string",
					Text: "Subject to publish on.",
				},
				{
					Name: "message",
					Type: "object",
					Text: `either a _string_ for :raw content, a _bag_ for JSON or SEN format,
or an sexpression for _lisp_ content.`,
				},
				{Name: "&optional"},
				{
					Name: "content-type",
					Type: "keyword",
					Text: `Content-type of the message which is in effect for encoding instances of the
 _bag-flavor_ and can be _:json_ or _:sen_.`,
				},
			},
			Return: "nil",
			Text: `__message-publish__ publish a message which is delivered to any _subscribers_
 matching the _subject_.`,
			Examples: []string{
				`(setq hub (make-app-hub)) => #<app-hub-flavor 1234>`,
				`(message-publish hub "subject" "a message") => nil`,
			},
		}, &Pkg)
}

// Publish represents the message-hub-close function.
type Publish struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *Publish) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.CheckArgCount(s, depth, f, args, 1, -1)
	inst, ok := args[0].(*flavors.Instance)
	if !ok {
		slip.TypePanic(s, depth, "hub", args[0], "instance")
	}
	return inst.Receive(s, ":publish", args[1:], depth)
}
