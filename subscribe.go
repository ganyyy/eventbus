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

const (
	statusInit     = iota // the subscribe Not executed, only use by once
	statusExecuted        // the subscribe is Executed or is executing, only use by once
	statusClosed          // the subscribe is closed
)

type subsCallback[T any] func(Param[T])

type subsChan[T any] chan Param[T]

type ISubsCall interface{ run(any) (success bool) }

// run
func (s subsCallback[T]) run(v any) bool {
	s(PackParam[T](v))
	return true
}

// run
func (s subsChan[T]) run(v any) bool {
	select {
	case s <- PackParam[T](v):
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

// Once
func Once() Opt { return (*subsOption).setOnce }

// Queue
func Queue() Opt { return (*subsOption).setQueue }

// Chan creates a new subscribe with a channel
func Chan[T any](notify chan Param[T]) ISubsCall {
	return subsChan[T](notify)
}

// Callback creates a new subscribe with a callback
func Callback[T any](cb func(Param[T])) ISubsCall {
	return subsCallback[T](cb)
}

func Any[T any, F interface {
	chan Param[T] | func(Param[T])
}](inner F) ISubsCall {
	switch v := (interface{})(inner).(type) {
	case chan Param[T]:
		return Chan(v)
	case func(Param[T]):
		return Callback(v)
	default:
		panic("invalid inner")
	}
}
