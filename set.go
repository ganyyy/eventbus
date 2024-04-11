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

var setEmpty struct{}

type Set[T comparable] map[T]struct{}

func NewSet[T comparable](cache uint) Set[T] {
	return make(Set[T], cache)
}

func NewSetNoCache[T comparable]() Set[T] {
	return NewSet[T](0)
}

// Add adds an element to the set.
func (s Set[T]) Add(e ...T) {
	for _, v := range e {
		s[v] = setEmpty
	}
}

// Remove removes an element from the set.
func (s Set[T]) Remove(e ...T) {
	if len(e) == 1 {
		delete(s, e[0])
		return
	}
	for _, v := range e {
		delete(s, v)
	}
}

// Contains checks if an element is in the set.
func (s Set[T]) Contains(e T) bool {
	_, ok := s[e]
	return ok
}

// Len returns the number of elements in the set.
func (s Set[T]) Len() int {
	return len(s)
}
