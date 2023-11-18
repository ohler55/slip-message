// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/gi"
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
		var replies gi.Channel
		if sv, ok := msg.(slip.Values); ok {
			msg = sv[0]
			replies, _ = sv[1].(gi.Channel)
		}
		msg = as.sub.convertMessage(msg)
		if as.sub.callback != nil {
			if reply := as.sub.callback.Call(s, slip.List{msg}, 0); reply != nil && replies != nil {
				as.reply(reply, replies)
			}
		}
	}
}

func (as *appSub) reply(msg slip.Object, replies gi.Channel) {
	defer func() {
		if rec := recover(); rec != nil {
			// Remain silent.
		}
	}()
	replies <- msg
}
