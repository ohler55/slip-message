// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/gi"
)

type appSub struct {
	sub   *subscription
	queue chan slip.Object
	done  chan struct{}
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
		msg = decodeMessage(msg, as.sub.contentType)
		if as.sub.callback != nil {
			if reply := as.sub.callback.Call(s, slip.List{msg}, 0); reply != nil && replies != nil {
				as.reply(reply, replies)
			}
		}
	}
	as.done <- struct{}{}
}

func (as *appSub) reply(msg slip.Object, replies gi.Channel) {
	defer func() {
		if rec := recover(); rec != nil {
			// Remain silent.
		}
	}()
	replies <- msg
}

func (as *appSub) shutdown() {
	as.queue <- nil
	<-as.done
}
