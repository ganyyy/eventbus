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
	"iter"
	"slices"
)

var empty struct{}

const (
	// use different sizes for the slice and map to balance the performance

	mixSetMaxSliceSize = 128 // when the set size is greater than this value, use map instead of slice
	mixSetMinMapSize   = 64  // when the set size is less than this value, use slice instead of map
)

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
		s[v] = empty
	}
}

// Remove removes an element from the set.
func (s Set[T]) Remove(e ...T) {
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

// AppendToSlice appends the elements in the set to a slice.
func (s Set[T]) AppendToSlice(slice []T) []T {
	slice = slices.Grow(slice, s.Len())
	for k := range s {
		slice = append(slice, k)
	}
	return slice
}

// Clear
func (s Set[T]) Clear() {
	clear(s)
}

// Range
func (s Set[T]) Range() iter.Seq[T] {
	return func(yield func(T) bool) {
		for k := range s {
			if !yield(k) {
				break
			}
		}
	}
}

// Transform transforms the set to a slice if the size is less than mixSetMinMapSize.
func (s Set[T]) Transform() ITransformSet[T] {
	if len(s) > mixSetMinMapSize {
		return s
	}
	set := NewSliceSet[T]()
	for k := range s {
		set.Add(k)
	}
	return set
}

type SliceSet[T comparable] struct {
	cache    [mixSetMaxSliceSize]T
	elements []T
}

// NewSliceSet creates a new set.
func NewSliceSet[T comparable]() *SliceSet[T] {
	var set SliceSet[T]
	set.elements = set.cache[:0]
	return &set
}

// Add adds an element to the set.
func (s *SliceSet[T]) Add(e ...T) {
	for _, v := range e {
		if s.Contains(v) {
			continue
		}
		s.elements = append(s.elements, v)
	}
}

// Contains checks if an element is in the set.
func (s *SliceSet[T]) Contains(e T) bool {
	return s.find(e) != -1
}

// find returns the index of an element in the set.
func (s *SliceSet[T]) find(e T) int {
	for i, v := range s.elements {
		if v == e {
			return i
		}
	}
	return -1
}

// Remove removes an element from the set.
func (s *SliceSet[T]) Remove(e ...T) {
	for _, v := range e {
		s.remove(v)
	}
}

// remove removes an element from the set.
func (s *SliceSet[T]) remove(e T) {
	idx := s.find(e)
	if idx == -1 {
		return
	}
	// swap the element to the end of the slice and truncate the slice
	lastIdx := len(s.elements) - 1
	var empty T
	s.elements[idx], s.elements[lastIdx] = s.elements[lastIdx], empty
	s.elements = s.elements[:lastIdx]
}

// Len returns the number of elements in the set.
func (s *SliceSet[T]) Len() int {
	return len(s.elements)
}

// AppendToSlice appends the elements in the set to a slice.
func (s *SliceSet[T]) AppendToSlice(slice []T) []T {
	slice = slices.Grow(slice, s.Len())
	return append(slice, s.elements...)
}

// Clear
func (s *SliceSet[T]) Clear() {
	clear(s.elements)
	s.elements = s.cache[:0]
}

// Range
func (s *SliceSet[T]) Range() iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s.elements {
			if !yield(v) {
				break
			}
		}
	}
}

// transform transforms the set to a map if the size is greater than mixSetMaxSliceSize.
func (s *SliceSet[T]) Transform() ITransformSet[T] {
	if s.Len() < mixSetMaxSliceSize {
		return s
	}
	set := NewSet[T](uint(s.Len()))
	set.Add(s.elements...)
	return set
}

// MixSet is a set that uses a slice for small sets and a map for large sets.
type MixSet[T comparable] struct{ ITransformSet[T] }

// NewMixSet creates a new set.
func NewMixSet[T comparable]() *MixSet[T] {
	s := InitMixSet[T]()
	return &s
}

// InitMixSet creates a new set with an initial capacity.
func InitMixSet[T comparable]() MixSet[T] {
	return MixSet[T]{ITransformSet: NewSliceSet[T]()}
}

// Transform transforms the set to a map if the size is greater than mixSetMaxSliceSize.
// or to a slice if the size is less than mixSetMinMapSize.
func (s *MixSet[T]) Transform() ITransformSet[T] {
	s.ITransformSet = s.ITransformSet.Transform()
	return s
}

// Add adds an element to the set.
func (s *MixSet[T]) Add(e ...T) {
	for _, v := range e {
		s.add(v)
	}
}

// add adds an element to the set.
func (s *MixSet[T]) add(e T) {
	s.ITransformSet.Add(e)
	s.Transform()
}

// Remove removes an element from the set.
func (s *MixSet[T]) Remove(e ...T) {
	for _, v := range e {
		s.remove(v)
	}
}

// remove removes an element from the set.
func (s *MixSet[T]) remove(e T) {
	s.ITransformSet.Remove(e)
	s.Transform()
}

// ISet is a set interface.
type ISet[T comparable] interface {
	Add(e ...T)
	Remove(e ...T)
	Contains(e T) bool
	Len() int
	AppendToSlice(slice []T) []T
	Clear()
	Range() iter.Seq[T]
}

// ITransformSet is a set interface that can transform to another set.
type ITransformSet[T comparable] interface {
	ISet[T]
	Transform() ITransformSet[T]
}

var (
	_ ISet[*Subscription] = Set[*Subscription]{}
	_ ISet[*Subscription] = &SliceSet[*Subscription]{}
	_ ISet[*Subscription] = &MixSet[*Subscription]{}
)
