// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import "github.com/nats-io/nats.go"

type jsSub struct {
	filter []string
	sub    *subscription
	nsub   *nats.Subscription
}
