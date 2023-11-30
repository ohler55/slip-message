// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

import "github.com/nats-io/nats.go"

type jsSub struct {
	sub  *subscription
	nsub *nats.Subscription
}
