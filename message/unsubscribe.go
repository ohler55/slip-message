// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := Unsubscribe{Function: slip.Function{Name: "message-unsubscribe", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "message-unsubscribe",
			Args: []*slip.DocArg{
				{
					Name: "hub",
					Type: "instance",
					Text: "The message hub to send the _:unsubscribe_ method.",
				},
				{
					Name: "subject",
					Type: "string|subscriber instance",
					Text: "Subject or subscriber to unsubscribe.",
				},
			},
			Return: "fixnum",
			Text:   `__message-unsubscribe__ returns the number of instances unsubscribed.`,
			Examples: []string{
				`(setq hub (make-app-hub)) => #<app-hub-flavor 1234>`,
				`(message-subscribe hub "subject" nil) => #<subscriber-flavor 1234>`,
				`(message-unsubscribe hub "subject") => 1`,
			},
		}, &Pkg)
}

// Unsubscribe represents the message-hub-close function.
type Unsubscribe struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *Unsubscribe) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.ArgCountCheck(f, args, 1, -1)
	inst, ok := args[0].(*flavors.Instance)
	if !ok {
		slip.PanicType("hub", args[0], "instance")
	}
	return inst.Receive(s, ":unsubscribe", args[1:], depth)
}
