// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main_test

import (
	"testing"

	"github.com/ohler55/slip/sliptest"
)

func TestPkgVars(t *testing.T) {
	(&sliptest.Function{
		Source: `*message*`,
		Expect: "#<package message>",
	}).Test(t)
}
