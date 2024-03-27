// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := Subscribers{Function: slip.Function{Name: "message-subscribers", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "message-subscribers",
			Args: []*slip.DocArg{
				{
					Name: "hub",
					Type: "instance",
					Text: "The message hub to send the _:subscribers_ method.",
				},
				{Name: "&optional"},
				{
					Name: "subject",
					Type: "string",
					Text: "Subject filter. The default is '*'.",
				},
			},
			Return: "list",
			Text:   `__message-subscribers__ returns the subscribers to the _subject_.`,
			Examples: []string{
				`(setq hub (make-app-hub)) => #<app-hub-flavor 1234>`,
				`(message-subscribe hub "subject" nil) => #<subscriber-flavor 1234>`,
				`(message-subscribers hub "subject") => (#<subscriber-flavor 1234>)`,
			},
		}, &Pkg)
}

// Subscribers represents the message-hub-close function.
type Subscribers struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *Subscribers) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.ArgCountCheck(f, args, 1, -1)
	inst, ok := args[0].(*flavors.Instance)
	if !ok {
		slip.PanicType("hub", args[0], "instance")
	}
	return inst.Receive(s, ":subscribers", args[1:], depth)
}
