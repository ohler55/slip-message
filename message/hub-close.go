// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := HubClose{Function: slip.Function{Name: "message-hub-close", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name: "message-hub-close",
			Args: []*slip.DocArg{
				{
					Name: "hub",
					Type: "instance",
					Text: "The message hub to close.",
				},
			},
			Return: "nil",
			Text:   `__message-hub-close__ closes a message hub instance.`,
			Examples: []string{
				`(make-app-hub) => #<app-hub-flavor 1234>`,
				`(message-hub-close) => nil`,
			},
		}, &Pkg)
}

// HubClose represents the message-hub-close function.
type HubClose struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *HubClose) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.ArgCountCheck(f, args, 1, 1)
	inst, ok := args[0].(*flavors.Instance)
	if !ok {
		slip.TypePanic(s, depth, "hub", args[0], "instance")
	}
	_ = inst.Receive(s, ":close", nil, depth)

	return nil
}
