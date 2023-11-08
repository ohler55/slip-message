// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
)

var (
	// Pkg is the message package.
	Pkg = slip.Package{
		Name:      "message",
		Nicknames: []string{"message", "msg"},
		Doc:       "Home of symbols defined for the message (msg) functions, variables, and constants.",
		Vars:      map[string]*slip.VarVal{},
		Lambdas:   map[string]*slip.Lambda{},
		Funcs:     map[string]*slip.FuncInfo{},
		PreSet:    slip.DefaultPreSet,
	}
)

func init() {
	slip.AddPackage(&Pkg)
	slip.UserPkg.Use(&Pkg)
	Pkg.Set("*message*", &Pkg)
	Pkg.Set("*msg*", &Pkg)
}
