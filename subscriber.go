// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"fmt"
	"strings"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	subscriberFlavor *flavors.Flavor
)

type subscription struct {
	hub         *flavors.Instance
	subject     string
	filter      []string
	contentType slip.Object
	callback    slip.Caller
}

func init() {
	subscriberFlavor = flavors.DefFlavor("parquet-subscriber-flavor",
		map[string]slip.Object{"filepath": nil},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":init-keywords"),
				slip.Symbol(":file"),
			},
			slip.Symbol(":gettable-instance-variables"),
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A parquet subscriber opens a parquet file and can be used to
access the content of that file.`),
			},
		},
	)
	subscriberFlavor.DefMethod(":init", "", subscriberInitCaller{})
	subscriberFlavor.DefMethod(":hub", "", subscriberHubCaller{})
	subscriberFlavor.DefMethod(":subject", "", subscriberSubjectCaller{})
	subscriberFlavor.DefMethod(":callback", "", subscriberCallbackCaller{})
	subscriberFlavor.DefMethod(":content-type", "", subscriberContentTypeCaller{})
	subscriberFlavor.DefMethod(":set-callback", "", subscriberSetCallbackCaller{})
	subscriberFlavor.DefMethod(":set-content-type", "", subscriberSetContentTypeCaller{})
	subscriberFlavor.DefMethod(":close", "", subscriberCloseCaller{})
}

type subscriberInitCaller struct{}

func (caller subscriberInitCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if 0 < len(args) {
		args = args[0].(slip.List)
	}
	var (
		hub     slip.Object
		subject slip.Object
		cb      slip.Object
		ct      slip.Object
	)
	for i := 0; i < len(args); i += 2 {
		key, _ := args[i].(slip.Symbol)
		switch string(key) {
		case ":hub":
			hub = args[i+1]
		case ":subject":
			subject = args[i+1]
		case ":callback":
			cb = args[i+1]
		case ":content-type":
			ct = args[i+1]
		default:
			slip.NewPanic("%s is not a valid keyword to subscriber-flavor :init", args[i])
		}
	}
	initSubscriber(self, hub, subject, cb, ct)

	return nil
}

func (caller subscriberInitCaller) Docs() string {
	return `__:init__ &key _hub_ _subject_ _callback_ _content-type_
   _:hub_ the message hub instance to handle the subscription.
   _:subject_ to listen on.
   _:callback_ can be either _nil_ when the _:next_ method will be called on a queue or
a function to call when a message is received.
   _:content-type_is an optional argument of the expected content type which can be one of
_nil_, _:auto_, _:raw_, _:json_, or _:lisp_. _nil_ is the same as _:auto_.

Sets the initial values when _make-instance_ is called.
`
}

type subscriberHubCaller struct{}

func (caller subscriberHubCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {
		result = sub.hub
	}
	return
}

func (caller subscriberHubCaller) Docs() string {
	return `__:hub__ => _instance_

Returns the hub instance this subscriber is subscribed through.
`
}

type subscriberSubjectCaller struct{}

func (caller subscriberSubjectCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {
		result = slip.String(sub.subject)
	}
	return
}

func (caller subscriberSubjectCaller) Docs() string {
	return `__:subject__ => _string_

Returns the subject this subscriber is subscribed to.
`
}

type subscriberCallbackCaller struct{}

func (caller subscriberCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {
		result, _ = sub.callback.(slip.Object)
	}
	return
}

func (caller subscriberCallbackCaller) Docs() string {
	return `__:callback__ => _function_

Returns the callback this subscriber invokes on receiving a message or nil if polling is expected with _:next_.
`
}

type subscriberContentTypeCaller struct{}

func (caller subscriberContentTypeCaller) Call(s *slip.Scope, args slip.List, _ int) (result slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {
		result = sub.contentType
	}
	return
}

func (caller subscriberContentTypeCaller) Docs() string {
	return `__:content-type__ => _keyword_|_nil_

Returns the content-type this subscriber expects.
`
}

type subscriberSetCallbackCaller struct{}

func (caller subscriberSetCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {
		sub.callback = cl.ResolveToCaller(&self.Scope, args[0], 0)
	}
	return nil
}

func (caller subscriberSetCallbackCaller) Docs() string {
	return `__:set-callback__ _function_ => _nil_

Sets the callback of this subscriber_.
`
}

type subscriberSetContentTypeCaller struct{}

func (caller subscriberSetContentTypeCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {
		sub.setContentType(args[0])
	}
	return nil
}

func (caller subscriberSetContentTypeCaller) Docs() string {
	return `__:set-content-type__ _nil_|_:json_|_:lisp_|_:auto_|_:raw_

Sets the content-type of this subscriber.
`
}

type subscriberCloseCaller struct{}

func (caller subscriberCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if sub, ok := self.Any.(*subscription); ok {

		fmt.Printf("*** sub: %v\n", sub)

		// TBD remove from hub, set hub to nil and zero string and filter

	}
	return nil
}

func (caller subscriberCloseCaller) Docs() string {
	return `__:close__ => _nil_

Closes the subscriber which stops the subscriber from listening on it's subject.
`
}

func initSubscriber(self *flavors.Instance, hub, subject, cb, ct slip.Object) {
	sub := subscription{}
	sub.setContentType(ct)
	if cb != nil {
		sub.callback = cl.ResolveToCaller(&self.Scope, cb, 0)
	}
	if hi, ok := hub.(*flavors.Instance); ok {
		sub.hub = hi
	} else {
		slip.PanicType(":hub", hub, "instance")
	}
	if ss, ok := subject.(slip.String); ok {
		sub.subject = string(ss)
		sub.filter = strings.Split(sub.subject, ".")
	} else {
		slip.PanicType(":subject", subject, "string")
	}
	self.Any = &sub

	// TBD add self to hub subscribers
	//  should be allowed but check that hubs match
}

func (sub *subscription) setContentType(ct slip.Object) {
	switch ct {
	case nil, slip.Symbol(":json"), slip.Symbol(":lisp"), slip.Symbol(":auto"), slip.Symbol(":raw"):
		sub.contentType = ct
	default:
		slip.PanicType(":content-type", ct, "nil", ":json", ":lisp", ":auto", ":raw")
	}
}
