// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
)

// Pkg is the message package.
var Pkg = slip.Package{
	Name:      "message",
	Nicknames: []string{"message", "msg"},
	Doc: `Home of symbols defined for the message (msg) functions, variables, and constants.


This package provided two kinds of message hubs. Both support publishing and
subscribing to subjects. The 'app' hub is an in-memory message hub while the
jetstream hub is a NATS messaging hub. Hubs are implemented with Slip Flavors
allowing additional hubs to be implemented such that they can be substituted
for either of the two hub types included in the package.


Messages can be either strings, JSON, or Lisp.


Example:
  (defvar greeting nil)
  (defvar my-hub (make-instance 'app-hub-flavor))
  (message-subscribe my-hub "hello.world" (lambda (m) (setq greeting m)))
  (message-publish my-hub "hello.world" "goodbye")

`,
	PreSet: slip.DefaultPreSet,
}

func init() {
	Pkg.Initialize(map[string]*slip.VarVal{
		"*message*": {
			Val:    &Pkg,
			Const:  true,
			Export: true,
			Doc:    `The message package.`,
		},
		"*msg*": {
			Val:    &Pkg,
			Const:  true,
			Export: true,
			Doc:    `The message package.`,
		},
	})
	defAppHubFlavor()
	defJetstreamHubFlavor()
	defSubscriberFlavor()

	Pkg.Initialize(nil, &stack{})
	slip.AddPackage(&Pkg)
	slip.UserPkg.Use(&Pkg)
}
