/*
Copyright 2024 eventbus Author(s)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package eventbus

import (
	"context"
	"sync/atomic"
)

const (
	statusInit     = iota // the subscribe Not executed, only use by once
	statusExecuted        // the subscribe is Executed or is executing, only use by once
	statusClosed          // the subscribe is closed
)

// replyFunc for request-reply subscribe
type replyFunc func(any) bool

type ICall interface {
	run(any, replyFunc) (success bool)
}

type callback[T any] func(Msg[T])

type channel[T any] chan<- Msg[T]

// run
func (s callback[T]) run(v any, reply replyFunc) bool {
	s(packMsg[T](v, reply))
	return true
}

// run
func (s channel[T]) run(v any, reply replyFunc) bool {
	select {
	case s <- packMsg[T](v, reply):
		return true
	default:
		return false
	}
}

type IReply interface {
	reply(any) bool
}

type Reply[T any] struct {
	resp chan Var[T]
	once atomic.Bool
}

func NewReply[T any]() *Reply[T] {
	return &Reply[T]{
		resp: make(chan Var[T], 1),
	}
}

// reply only once response
func (r *Reply[T]) reply(resp any) bool {
	if !r.once.CompareAndSwap(false, true) {
		return false
	}
	select {
	case r.resp <- Var[T]{v: resp}:
		return true
	default:
		return false
	}
}

// Resp
func (r *Reply[T]) Resp(ctx context.Context) (Var[T], bool) {
	select {
	case v := <-r.resp:
		return v, true
	case <-ctx.Done():
		return Var[T]{}, false
	}
}

type Msg[T any] struct {
	Var[T]
	reply replyFunc
}

func packMsg[T any](val any, reply replyFunc) Msg[T] {
	return Msg[T]{
		Var:   Var[T]{v: val},
		reply: reply,
	}
}

// Reply
func (o Msg[T]) Reply(val any) bool {
	if o.reply != nil {
		return o.reply(val)
	}
	return false
}

type Var[T any] struct{ v any }

// Val
func (v Var[T]) Val() T {
	val, _ := v.Raw().(T)
	return val
}

// Raw
func (v Var[T]) Raw() any { return v.v }

type Opt func(*subsOption)

type subsOption struct {
	once  bool // once subscribe
	queue bool // TODO queue subscribe
}

// setOnce
func (o *subsOption) setOnce() { o.once = true }

// setQueue
func (o *subsOption) setQueue() { o.queue = true }

// apply
func (o *subsOption) apply(opts ...Opt) {
	for _, opt := range opts {
		opt(o)
	}
}

// Once
func Once() Opt { return (*subsOption).setOnce }

// Queue
func Queue() Opt { return (*subsOption).setQueue }

// Chan creates a new subscribe with a channel
func Chan[T any](notify chan<- Msg[T]) ICall {
	return channel[T](notify)
}

// MakeChan
func MakeChan[T any](capacity int) (tx chan<- Msg[T], rx <-chan Msg[T]) {
	ch := make(chan Msg[T], capacity)
	return ch, ch
}

// Callback creates a new subscribe with a callback
func Callback[T any](cb func(Msg[T])) ICall {
	return callback[T](cb)
}

func Any[T any, F chan Msg[T] | func(Msg[T])](inner F) ICall {
	switch v := (interface{})(inner).(type) {
	case chan Msg[T]:
		return Chan(v)
	case func(Msg[T]):
		return Callback(v)
	default:
		panic("invalid inner")
	}
}

// CallbackSubs
func CallbackSubs[T any](topic string, cb func(Msg[T]), opts ...Opt) *Subscription {
	return NewSubs(topic, Callback(cb), opts...)
}

// ChanSubs
func ChanSubs[T any](topic string, capacity int, opts ...Opt) (rx <-chan Msg[T], sub *Subscription) {
	tx, rx := MakeChan[T](capacity)
	sub = NewSubs(topic, Chan(tx), opts...)
	return rx, sub
}
