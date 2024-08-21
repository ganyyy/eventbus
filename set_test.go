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
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {

	t.Run("Common", func(t *testing.T) {
		ts := func(s ISet[int]) {
			// Test Add
			s.Add(1, 2, 3)
			require.Equal(t, 3, s.Len())
			s.Add(1, 2, 3)
			require.Equal(t, 3, s.Len())

			// Test Contains
			require.True(t, s.Contains(1))
			require.True(t, s.Contains(2))
			require.True(t, s.Contains(3))
			require.False(t, s.Contains(4))

			// Test Remove
			s.Remove(2, 4)
			require.False(t, s.Contains(2))

			// Test Len after Remove
			require.Equal(t, 2, s.Len())

			// Test Clear
			s.Clear()
			require.Equal(t, 0, s.Len())

			// Test Range
			var origin = []int{1, 2, 3}
			s.Add(origin...)
			sum := 0
			for ele := range s.Range() {
				sum += ele
			}
			var after = s.AppendToSlice(nil)
			sort.IntSlice(after).Sort()
			require.Equal(t, origin, after)
			require.Equal(t, 6, sum)

			for ele := range s.Range() {
				if ele == 2 {
					break
				}
			}
		}

		ts(NewSet[int](0))
		ts(NewSliceSet[int]())
		ts(NewMixSet[int]())
	})

	t.Run("MixSet", func(t *testing.T) {
		s := NewMixSet[int]()

		var setType = reflect.TypeOf((Set[int])(nil))
		var sliceType = reflect.TypeOf((*SliceSet[int])(nil))

		checkType := func(tt reflect.Type) {
			sType := reflect.TypeOf(s.ITransformSet)
			require.Equal(t, sType, tt)
		}

		checkType(sliceType)
		for i := 0; i < mixSetMaxSliceSize-1; i++ {
			s.Add(i)
		}

		checkType(sliceType)
		require.Equal(t, mixSetMaxSliceSize-1, s.Len())
		s.Add(mixSetMaxSliceSize - 1)
		checkType(setType)
		require.Equal(t, mixSetMaxSliceSize, s.Len())

		for i := 0; i < mixSetMaxSliceSize-mixSetMinMapSize-1; i++ {
			s.Remove(i)
		}
		checkType(setType)
		require.Equal(t, mixSetMinMapSize+1, s.Len())
		s.Remove(mixSetMaxSliceSize - 1)
		checkType(sliceType)
		require.Equal(t, mixSetMinMapSize, s.Len())
	})
}

func BenchmarkTransform(b *testing.B) {
	b.Run("Slice2Set", func(b *testing.B) {
		var s = NewSliceSet[int]()

		for i := 0; i < mixSetMaxSliceSize; i++ {
			s.Add(i)
		}

		for i := 0; i < b.N; i++ {
			_ = s.Transform()
		}
	})
	b.ResetTimer()
	b.ReportAllocs()

	b.Run("Set2Slice", func(b *testing.B) {
		var s = NewSet[int](mixSetMinMapSize)
		for i := 0; i < mixSetMinMapSize; i++ {
			s.Add(i)
		}

		for i := 0; i < b.N; i++ {
			_ = s.Transform()
		}
	})
	b.ReportAllocs()
}
