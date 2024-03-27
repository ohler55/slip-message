// Copyright (c) 2023, Peter Ohler, All rights reserved.

package message

import (
	"time"

	"github.com/ohler55/slip"
)

type stack struct {
	name    string
	new     chan string
	pending []int64
	cnt     int64
	tsum    int64
}

// Must be mu.Locked first.
func (st *stack) appendAssoc(list slip.List) slip.List {
	list = append(list, slip.List{slip.Symbol("name"), slip.Tail{Value: slip.String(st.name)}})
	list = append(list, slip.List{slip.Symbol("queued"), slip.Tail{Value: slip.Fixnum(len(st.new))}})
	var pending slip.Fixnum
	for _, t := range st.pending {
		if t != 0 {
			pending++
		}
	}
	list = append(list, slip.List{slip.Symbol("pending"), slip.Tail{Value: pending}})
	list = append(list, slip.List{slip.Symbol("acked"), slip.Tail{Value: slip.Fixnum(st.cnt)}})
	list = append(list, slip.List{slip.Symbol("average-ack"), slip.Tail{Value: slip.Fixnum(st.averageAck())}})

	return list
}

func (st *stack) averageAck() (average time.Duration) {
	if 0 < st.cnt {
		average = time.Duration(st.tsum / st.cnt)
	}
	return
}
