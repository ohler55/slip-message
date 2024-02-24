// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"strings"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	subscriberFlavor *flavors.Flavor
)

func init() {
	Pkg.Initialize(nil)
	subscriberFlavor = flavors.DefFlavor("subscriber-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A message subscriber is an object that facilitates listening
for message distributed by a message hub.`),
			},
		},
		&Pkg,
	)
	subscriberFlavor.Final = true
	subscriberFlavor.GoMakeOnly = true
	subscriberFlavor.DefMethod(":hub", "", subscriberHubCaller{})
	subscriberFlavor.DefMethod(":subject", "", subscriberSubjectCaller{})
	subscriberFlavor.DefMethod(":callback", "", subscriberCallbackCaller{})
	subscriberFlavor.DefMethod(":name", "", subscriberNameCaller{})
	subscriberFlavor.DefMethod(":content-type", "", subscriberContentTypeCaller{})
	subscriberFlavor.DefMethod(":set-callback", "", subscriberSetCallbackCaller{})
	subscriberFlavor.DefMethod(":set-content-type", "", subscriberSetContentTypeCaller{})
	subscriberFlavor.DefMethod(":close", "", subscriberCloseCaller{})
	subscriberFlavor.DefMethod(":next", "", subscriberNextCaller{})
}

type subscriberHubCaller struct{}

func (caller subscriberHubCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return self.Any.(*subscription).hub
}

func (caller subscriberHubCaller) Docs() string {
	return `__:hub__ => _instance_


Returns the hub instance this subscriber is subscribed through.
`
}

type subscriberSubjectCaller struct{}

func (caller subscriberSubjectCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return slip.String(strings.Join(self.Any.(*subscription).subject, "."))
}

func (caller subscriberSubjectCaller) Docs() string {
	return `__:subject__ => _string_


Returns the subject this subscriber is subscribed to.
`
}

type subscriberCallbackCaller struct{}

func (caller subscriberCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) (cb slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if self.Any.(*subscription).callback != nil {
		cb = self.Any.(*subscription).callback.(slip.Object)
	}
	return
}

func (caller subscriberCallbackCaller) Docs() string {
	return `__:callback__ => _function_


Returns the callback this subscriber invokes on receiving a message or nil if polling is expected with _:next_.
`
}

type subscriberContentTypeCaller struct{}

func (caller subscriberContentTypeCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return self.Any.(*subscription).contentType
}

func (caller subscriberContentTypeCaller) Docs() string {
	return `__:content-type__ => _keyword_|_nil_


Returns the content-type this subscriber expects.
`
}

type subscriberNameCaller struct{}

func (caller subscriberNameCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return slip.String(self.Any.(*subscription).name)
}

func (caller subscriberNameCaller) Docs() string {
	return `__:name__ => _string_


Returns the name this subscriber is subscribed to.
`
}

type subscriberSetCallbackCaller struct{}

func (caller subscriberSetCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	if args[0] == nil {
		self.Any.(*subscription).callback = nil
	} else {
		self.Any.(*subscription).callback = cl.ResolveToCaller(&self.Scope, args[0], 0)
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
	self.Any.(*subscription).setContentType(args[0])
	return nil
}

func (caller subscriberSetContentTypeCaller) Docs() string {
	return `__:set-content-type__ _nil_|_:json_|_:lisp_|_:auto_|_:raw_


Sets the content-type of this subscriber.
`
}

type subscriberCloseCaller struct{}

func (caller subscriberCloseCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	sub := self.Any.(*subscription)
	sub.hub.Receive(s, ":unsubscribe", slip.List{sub.self}, depth)
	return nil
}

func (caller subscriberCloseCaller) Docs() string {
	return `__:close__ => _nil_


Closes the subscriber which stops the subscriber from listening on it's subject.
`
}

type subscriberNextCaller struct{}

func (caller subscriberNextCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	sub := self.Any.(*subscription)
	if 0 < len(args) {
		return sub.hub.Receive(s, ":next", slip.List{self, slip.Symbol(":timeout"), args[0]}, depth)
	}
	return sub.hub.Receive(s, ":next", slip.List{self}, depth)
}

func (caller subscriberNextCaller) Docs() string {
	return `__:next__ &optional _timeout_=> _object_, _fixnum_


Get the next message on a queue and return the message and message identifier.
`
}

func makeSubscriber(hub *flavors.Instance, subject, cb, ct, name slip.Object) (
	self *flavors.Instance, sub *subscription, subjStr string) {

	self = subscriberFlavor.MakeInstance().(*flavors.Instance)
	sub = &subscription{self: self, hub: hub}
	sub.setContentType(ct)
	if cb != nil {
		sub.callback = cl.ResolveToCaller(&self.Scope, cb, 0)
	}
	if ss, ok := subject.(slip.String); ok {
		subjStr = string(ss)
		sub.subject = strings.Split(string(ss), ".")
	} else {
		slip.PanicType(":subject", subject, "string")
	}
	if name != nil {
		if ss, ok := name.(slip.String); ok {
			sub.name = string(ss)
		} else {
			slip.PanicType(":name", name, "string")
		}
	}
	self.Any = sub

	return
}
