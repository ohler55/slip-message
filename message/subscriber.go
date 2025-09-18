// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"strings"

	"github.com/ohler55/slip"
	"github.com/ohler55/slip/pkg/cl"
	"github.com/ohler55/slip/pkg/flavors"
)

var (
	subscriberFlavor *flavors.Flavor
)

func defSubscriberFlavor() {
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

func (caller subscriberHubCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":hub",
		Text:   `Returns the hub instance this subscriber is subscribed through.`,
		Return: "app-hub-flavor|jetstream-hub-flavor",
	}
}

type subscriberSubjectCaller struct{}

func (caller subscriberSubjectCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return slip.String(strings.Join(self.Any.(*subscription).subject, "."))
}

func (caller subscriberSubjectCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":subject",
		Text:   `Returns the subject this subscriber is subscribed to.`,
		Return: "string",
	}
}

type subscriberCallbackCaller struct{}

func (caller subscriberCallbackCaller) Call(s *slip.Scope, args slip.List, _ int) (cb slip.Object) {
	self := s.Get("self").(*flavors.Instance)
	if self.Any.(*subscription).callback != nil {
		cb = self.Any.(*subscription).callback.(slip.Object)
	}
	return
}

func (caller subscriberCallbackCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":callback",
		Text: `Returns the callback this subscriber invokes on receiving a message or nil
if polling is expected with _:next_.`,
		Return: "function",
	}
}

type subscriberContentTypeCaller struct{}

func (caller subscriberContentTypeCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return self.Any.(*subscription).contentType
}

func (caller subscriberContentTypeCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":content-type",
		Text:   `Returns the content-type this subscriber expects.`,
		Return: "keyword|nil",
	}
}

type subscriberNameCaller struct{}

func (caller subscriberNameCaller) Call(s *slip.Scope, args slip.List, _ int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	return slip.String(self.Any.(*subscription).name)
}

func (caller subscriberNameCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name:   ":name",
		Text:   `Returns the name this subscriber is subscribed to.`,
		Return: "string",
	}
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

func (caller subscriberSetCallbackCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":set-callback",
		Text: `Sets the callback of this subscriber.`,
		Args: []*slip.DocArg{
			{
				Name: "function",
				Type: "function",
				Text: "The function to use as the callback for the subscriber.",
			},
		},
	}
}

type subscriberSetContentTypeCaller struct{}

func (caller subscriberSetContentTypeCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	self.Any.(*subscription).setContentType(s, args[0], depth)
	return nil
}

func (caller subscriberSetContentTypeCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":set-content-type",
		Text: `Sets the content-type of this subscriber.`,
		Args: []*slip.DocArg{
			{
				Name: "type",
				Type: "keyword",
				Text: "_nil_|_:json_|_:lisp_|_:auto_|_:raw_",
			},
		},
	}
}

type subscriberCloseCaller struct{}

func (caller subscriberCloseCaller) Call(s *slip.Scope, args slip.List, depth int) slip.Object {
	self := s.Get("self").(*flavors.Instance)
	sub := self.Any.(*subscription)
	sub.hub.Receive(s, ":unsubscribe", slip.List{sub.self}, depth)
	return nil
}

func (caller subscriberCloseCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":close",
		Text: `Closes the subscriber which stops the subscriber from listening on it's subject.`,
	}
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

func (caller subscriberNextCaller) FuncDocs() *slip.FuncDoc {
	return &slip.FuncDoc{
		Name: ":next",
		Text: `Sets the content-type of this subscriber.`,
		Args: []*slip.DocArg{
			{Name: "&optional"},
			{
				Name: "timeout",
				Type: "real",
				Text: "The number of seconds to wait for the next message.",
			},
		},
		Return: "bag, fixnum",
	}
}

func makeSubscriber(s *slip.Scope, hub *flavors.Instance, subject, cb, ct, name slip.Object, depth int) (
	self *flavors.Instance, sub *subscription, subjStr string) {

	self = subscriberFlavor.MakeInstance().(*flavors.Instance)
	sub = &subscription{self: self, hub: hub}
	sub.setContentType(s, ct, depth)
	if cb != nil {
		sub.callback = cl.ResolveToCaller(&self.Scope, cb, 0)
	}
	if ss, ok := subject.(slip.String); ok {
		subjStr = string(ss)
		sub.subject = strings.Split(string(ss), ".")
	} else {
		slip.TypePanic(s, depth, ":subject", subject, "string")
	}
	if name != nil {
		if ss, ok := name.(slip.String); ok {
			sub.name = string(ss)
		} else {
			slip.TypePanic(s, depth, ":name", name, "string")
		}
	}
	self.Any = sub

	return
}
