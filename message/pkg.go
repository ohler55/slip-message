// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
)

// Pkg is the message package.
var Pkg = slip.Package{
	Name:      "message",
	Nicknames: []string{"message", "msg"},
	Doc:       "Home of symbols defined for the message (msg) functions, variables, and constants.",
	PreSet:    slip.DefaultPreSet,
}

func init() {
	Pkg.Initialize(map[string]*slip.VarVal{})
	slip.DefConstant(slip.Symbol("*message*"), &Pkg, "")
	slip.DefConstant(slip.Symbol("*msg*"), &Pkg, "")
	defAppHubFlavor()
	defJetstreamHubFlavor()
	defSubscriberFlavor()

	Pkg.Initialize(nil, &stack{})
	slip.AddPackage(&Pkg)
	slip.UserPkg.Use(&Pkg)
}
