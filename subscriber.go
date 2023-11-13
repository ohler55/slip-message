// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import (
	"fmt"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	subscriberFlavor *flavors.Flavor
)

func init() {
	subscriberFlavor = flavors.DefFlavor("subscriber-flavor",
		map[string]slip.Object{},
		nil,
		slip.List{
			slip.List{
				slip.Symbol(":init-keywords"),
				slip.Symbol(":hub"),
				slip.Symbol(":subject"),
				slip.Symbol(":callback"),
				slip.Symbol(":content-type"),
			},
			slip.List{
				slip.Symbol(":documentation"),
				slip.String(`A message subscriber is an object that facilitates listening
for message distributed by a message hub.`),
			},
		},
	)
	subscriberFlavor.Final = true
	subscriberFlavor.GoMakeOnly = true
	subscriberFlavor.DefMethod(":hub", "", subscriberHubCaller{})
	subscriberFlavor.DefMethod(":subject", "", subscriberSubjectCaller{})
	subscriberFlavor.DefMethod(":callback", "", subscriberCallbackCaller{})
	subscriberFlavor.DefMethod(":content-type", "", subscriberContentTypeCaller{})
	subscriberFlavor.DefMethod(":set-callback", "", subscriberSetCallbackCaller{})
	subscriberFlavor.DefMethod(":set-content-type", "", subscriberSetContentTypeCaller{})
	subscriberFlavor.DefMethod(":close", "", subscriberCloseCaller{})
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
	return slip.String(self.Any.(*subscription).subject)
}

func (caller subscriberSubjectCaller) Docs() string {
	return `__:subject__ => _string_


Returns the subject this subscriber is subscribed to.
`
}

type subscriberCallbackCaller struct{}

func (caller subscriberCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return self.Any.(*subscription).callback.(slip.Object)
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

type subscriberSetCallbackCaller struct{}

func (caller subscriberSetCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any.(*subscription).callback = cl.ResolveToCaller(&self.Scope, args[0], 0)
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

func (caller subscriberCloseCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	sub := self.Any.(*subscription)

	fmt.Printf("*** sub: %v\n", sub)

	// TBD remove from hub, set hub to nil

	return nil
}

func (caller subscriberCloseCaller) Docs() string {
	return `__:close__ => _nil_


Closes the subscriber which stops the subscriber from listening on it's subject.
`
}

func makeSubscriber(hub, subject, cb, ct slip.Object) (self *flavors.Instance, sub *subscription) {
	self = subscriberFlavor.MakeInstance().(*flavors.Instance)
	sub = &subscription{self: self}
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
	} else {
		slip.PanicType(":subject", subject, "string")
	}
	self.Any = sub

	return
}
