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
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// T must pointer type
type Pool[T any] struct {
	// Reset resets a T and returns true if it can be reused.
	// If is nil, it will not be reset and put back to the pool.
	Reset func(T) bool
	pool  *sync.Pool
}

func NewPool[T any](newFunc func() T, resetFunc func(T) bool) *Pool[T] {
	return &Pool[T]{
		Reset: resetFunc,
		pool: &sync.Pool{
			New: func() any {
				return newFunc()
			},
		},
	}
}

// Get returns a T from the pool or create a new T.
func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put puts a T back to the pool.
func (p *Pool[T]) Put(v T) {
	if p.Reset == nil || p.Reset(v) {
		p.pool.Put(v)
	}
}

func newRand() func() *rand.Rand {
	var inc atomic.Uint64
	return func() *rand.Rand {
		return rand.New(
			rand.NewPCG(
				uint64(time.Now().UnixNano()),
				inc.Add(1)),
		)
	}
}

var randPool = NewPool(
	newRand(), nil,
)

func randIdx(length int) int {
	r := randPool.Get()
	idx := r.IntN(length)
	randPool.Put(r)
	return idx
}

func shuffleSlices[T any, S ~[]T](s S) {
	r := randPool.Get()
	r.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	randPool.Put(r)
}
