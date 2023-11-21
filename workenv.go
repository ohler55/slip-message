// Copyright (c) 2023, Peter Ohler, All rights reserved.

package main

type envStatus byte

const (
	newStatus envStatus = iota
	pendingStatus
	ackedStatus
)

type workEnv struct {
	mid    int64
	msg    string
	status envStatus
}
