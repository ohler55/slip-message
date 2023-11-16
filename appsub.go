// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"fmt"

	"github.com/ohler55/slip"
)

type appSub struct {
	filter []string
	sub    *subscription
	queue  chan slip.Object
}

func (as *appSub) loop(s *slip.Scope) {
	for {
		msg := <-as.queue
		if msg == nil {
			break
		}
		msg = as.sub.convertMessage(msg)
		if as.sub.callback != nil {
			_ = as.sub.callback.Call(s, slip.List{msg}, 0)
		} else {
			// TBD if caller is nil then keep on queue or maybe keep a list
			fmt.Printf("*** %s msg %s\n", as.sub.subject, msg)
		}
	}
}
