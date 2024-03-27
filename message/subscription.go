// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/ojg/sen"
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/bag"
	"github.com/ohler55/slip/pkg/flavors"
)

type subscription struct {
	hub         *flavors.Instance
	subject     []string
	contentType slip.Object
	callback    slip.Caller
	name        string
	self        *flavors.Instance
}

func (sub *subscription) setContentType(ct slip.Object) {
	switch ct {
	case nil, slip.Symbol(":json"), slip.Symbol(":lisp"), slip.Symbol(":auto"), slip.Symbol(":raw"):
		sub.contentType = ct
	default:
		slip.PanicType(":content-type", ct, "nil", ":json", ":lisp", ":auto", ":raw")
	}
}

func decodeMessage(msg, contentType slip.Object) slip.Object {
	if ss, ok := msg.(slip.String); ok && 0 < len(ss) {
		switch contentType {
		case nil, slip.Symbol(":auto"):
			switch ss[0] {
			case '{', '[':
				inst := bag.Flavor().MakeInstance().(*flavors.Instance)
				inst.Any = sen.MustParse([]byte(ss))
				msg = inst
			case '(':
				if code, _ := slip.ReadOne([]byte(ss)); 0 < len(code) {
					msg = code[0]
				}
			}
		case slip.Symbol(":json"):
			inst := bag.Flavor().MakeInstance().(*flavors.Instance)
			inst.Any = sen.MustParse([]byte(ss))
			msg = inst
		case slip.Symbol(":lisp"):
			if code, _ := slip.ReadOne([]byte(ss)); 0 < len(code) {
				msg = code[0]
			}
		case slip.Symbol(":raw"):
			// leave as raw
		}
	}
	return msg
}
