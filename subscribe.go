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
	"sync/atomic"
)

const (
	statusInit     = iota // the subscribe Not executed, only use by once
	statusExecuted        // the subscribe is Executed or is executing, only use by once
	statusClosed          // the subscribe is closed
)

type subsInner struct {
	subsOption
	status atomic.Int32
}

// isOnce returns true if the subscribe is once
func (s *subsInner) isOnce() bool { return s.once }

// CanCall returns true if the subscribe can be called
func (s *subsInner) CanCall() bool {
	// if the subscribe is done, return false
	// if the subscribe is once and executed, return false
	return !s.isDone() &&
		(!s.isOnce() ||
			s.status.CompareAndSwap(
				statusInit, statusExecuted,
			))
}

// stop stops the subscribe
func (s *subsInner) stop() {
	s.status.Store(statusClosed)
}

// isDone returns true if the subscribe is closed
func (s *subsInner) isDone() bool {
	return s.status.Load() == statusClosed
}

type subsCallback[T any] struct {
	subsInner
	cb func(Param[T])
}

type subsChan[T any] struct {
	subsInner
	notify chan Param[T]
}

type ISubscribe interface {
	isOnce() bool
	isDone() bool
	call(any) (success bool)
	stop()
}

// call
func (s *subsCallback[T]) call(v any) bool {
	if !s.CanCall() {
		return false
	}
	s.cb(PackParam[T](v))
	return true
}

// call
func (s *subsChan[T]) call(v any) bool {
	if !s.CanCall() {
		return false
	}
	select {
	case s.notify <- PackParam[T](v):
		return true
	default:
		return false
	}
}

type Param[T any] struct{ v any }

// Val
func (o Param[T]) Val() T {
	val, _ := o.Raw().(T)
	return val
}

// Raw
func (o Param[T]) Raw() any {
	return o.v
}

func PackParam[T any](val any) Param[T] {
	return Param[T]{val}
}

type Opt func(*subsOption)

type subsOption struct {
	once  bool // once subscribe
	queue bool // queue subscribe
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

// genOption
func genOption(opts ...Opt) subsOption {
	var opt subsOption
	opt.apply(opts...)
	return opt
}

// Once
func Once() Opt { return (*subsOption).setOnce }

// Queue
func Queue() Opt { return (*subsOption).setQueue }

// Chan creates a new subscribe with a channel
func Chan[T any](notify chan Param[T], opts ...Opt) ISubscribe {
	return &subsChan[T]{
		notify: notify,
		subsInner: subsInner{
			subsOption: genOption(opts...),
		},
	}
}

// Callback creates a new subscribe with a callback
func Callback[T any](cb func(Param[T]), opts ...Opt) ISubscribe {
	return &subsCallback[T]{
		cb: cb,
		subsInner: subsInner{
			subsOption: genOption(opts...),
		},
	}
}

func Any[T any, F interface {
	chan Param[T] | func(Param[T])
}](inner F, opts ...Opt) ISubscribe {
	switch v := (interface{})(inner).(type) {
	case chan Param[T]:
		return Chan(v, opts...)
	case func(Param[T]):
		return Callback(v, opts...)
	default:
		panic("invalid inner")
	}
}
