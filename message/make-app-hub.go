// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/flavors"
)

func init() {
	slip.Define(
		func(args slip.List) slip.Object {
			f := MakeAppHub{Function: slip.Function{Name: "make-app-hub", Args: args}}
			f.Self = &f
			return &f
		},
		&slip.FuncDoc{
			Name:   "make-app-hub",
			Args:   []*slip.DocArg{},
			Return: "instance",
			Text:   `__make-app-hub__ creates a new _app-hub-flavor_ instance.`,
			Examples: []string{
				`(make-app-hub) => #<app-hub-flavor 1234>`,
			},
		}, &Pkg)
}

// MakeAppHub represents the make-app-hub function.
type MakeAppHub struct {
	slip.Function
}

// Call the function with the arguments provided.
func (f *MakeAppHub) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	slip.CheckArgCount(s, depth, f, args, 0, 0)
	inst := appHubFlavor.MakeInstance().(*flavors.Instance)
	inst.Any = &appHub{}

	return inst
}
