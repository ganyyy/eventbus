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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Subject(sub string) *Subscription {
	return &Subscription{subject: sub, inner: Callback[int](nil)}
}

func TestSublist(t *testing.T) {
	defaultSublistWithSubs := func() (*Sublist, []*Subscription) {
		// Create a new sublist
		sublist := NewSublist()
		// Insert some subscriptions
		subs := []*Subscription{
			Subject("a"),
			Subject("a.b"),
			Subject("a.b.c"),
		}
		for _, sub := range subs {
			sublist.Insert(sub)
		}
		return sublist, subs

	}

	defaultSublist := func() *Sublist {
		// Create a new sublist
		sublist, _ := defaultSublistWithSubs()
		return sublist
	}

	t.Run("Insert", func(t *testing.T) {
		var sublist = defaultSublist()

		require.NoError(t, sublist.Insert(Subject("a")))
		require.Error(t, sublist.Insert(Subject("a.")))
		require.Error(t, sublist.Insert(Subject("")))

		require.Equal(t, uint64(4), sublist.Count.Load())
	})

	t.Run("Match", func(t *testing.T) {
		var sublist, subs = defaultSublistWithSubs()

		var subSet = make(map[string]*Subscription)
		for _, sub := range subs {
			subSet[sub.subject] = sub
		}

		var matchCases = []struct {
			subject string
		}{
			{subject: "a"},
			{subject: "a.b"},
			{subject: "a.b.c"},
			{subject: "a.b.c.d"},
			{subject: "a.c.d"},
			{subject: "a.c."},
			{subject: ""},
		}

		var matchTotal = 0
		for _, v := range matchCases {
			ret := sublist.match(v.subject)
			want := subSet[v.subject]
			require.Equal(t, want != nil, len(ret.psubs) == 1)
			if want != nil {
				require.Equal(t, want, ret.psubs[0])
			}
			if ValidSubject(v.subject) {

				matchTotal++
			}
			require.Equal(t, matchTotal, int(sublist.Matches.Load()))
		}
	})

	t.Run("MatchMany", func(t *testing.T) {
		var sublist = NewSublist()
		const TOTAL = PListCacheMin * 2
		var subSet = NewSet[*Subscription](0)
		for i := 0; i < TOTAL; i++ {
			sub := Subject("a")
			subSet.Add(sub)
			sublist.Insert(sub)
		}
		require.Equal(t, TOTAL, subSet.Len())
		require.Equal(t, uint64(TOTAL), sublist.Count.Load())

		ret := sublist.match("a")
		require.Equal(t, TOTAL, len(ret.psubs))
		for _, sub := range ret.psubs {
			require.True(t, subSet.Contains(sub))
		}
		require.Equal(t, uint64(1), sublist.Matches.Load())

		require.NoError(t, sublist.Remove(ret.psubs[0]))
	})

	t.Run("Remove", func(t *testing.T) {
		var sublist, subs = defaultSublistWithSubs()
		require.Equal(t, uint64(3), sublist.Count.Load())

		require.ErrorIs(t, sublist.Remove(Subject("a.")), ErrInvalidSubject)
		require.ErrorIs(t, sublist.Remove(Subject("a")), ErrNotFound)
		require.NoError(t, sublist.Remove(subs[0]))
		require.Equal(t, uint64(2), sublist.Count.Load())
		require.Equal(t, uint64(1), sublist.Removes.Load())

		require.ErrorIs(t, sublist.Remove(Subject("a.c")), ErrNotFound)
		require.ErrorIs(t, sublist.RemoveBatch(subs[:1]), ErrNotFound)

		require.NoError(t, sublist.RemoveBatch(subs[1:]))
		require.Equal(t, uint64(0), sublist.Count.Load())
		require.Equal(t, uint64(3), sublist.Removes.Load())

		require.ErrorIs(t, sublist.RemoveBatch(subs), ErrNotFound)

		err := sublist.RemoveBatch(append(subs, Subject("a.")))
		require.ErrorIs(t, err, ErrInvalidSubject)
		require.ErrorIs(t, err, ErrNotFound)

		require.ErrorIs(t, sublist.RemoveBatch([]*Subscription{
			Subject(""),
			Subject("a."),
			Subject(".a"),
			Subject("a.b."),
		}), ErrInvalidSubject)

		require.NoError(t, sublist.RemoveBatch(nil))
	})

	t.Run("RemoveBatch", func(t *testing.T) {
		sublist := NewSublist()

		const subject = "a"
		const NUM = 200
		var allSubject = make([]*Subscription, 0, NUM)
		for i := 0; i < NUM; i++ {
			sub := Subject(subject)
			allSubject = append(allSubject, sub)
			require.NoError(t, sublist.Insert(sub))
		}
		require.Equal(t, uint64(NUM), sublist.Count.Load())

		require.NoError(t, sublist.RemoveBatch(allSubject))
		require.Equal(t, uint64(0), sublist.Count.Load())
		require.Equal(t, uint64(NUM), sublist.Removes.Load())

	})
}

func TestMultiSublist(t *testing.T) {

	t.Run("Subscribe", func(t *testing.T) {

		emptyCallback := Callback[int](nil)
		emptyChan := Chan[int](nil)

		var ms = NewMultiSublist(1)
		require.NoError(t, ms.Subscribe(NewSubs("a", emptyCallback)))
		require.NoError(t, ms.Subscribe(NewSubs("a", emptyChan)))

		require.ErrorIs(t, ms.Subscribe(NewSubs("", emptyCallback)), ErrInvalidSubject)
		require.ErrorIs(t, ms.Subscribe(NewSubs("a.", emptyCallback)), ErrInvalidSubject)
	})

	t.Run("Publish", func(t *testing.T) {
		var ms = NewMultiSublist(1)

		var cbRet int
		var subCB = NewSubs("a", Callback(func(p Param[int]) {
			cbRet = p.Val()
		}))

		var retChan = make(chan Param[int], 1)
		var subChan = NewSubs("a", Chan(retChan, Once()))

		require.NoError(t, ms.Subscribe(subCB))
		require.NoError(t, ms.Subscribe(subChan))

		snmp := ms.Snmp()
		require.Equal(t, uint64(2), snmp.Count.Load())
		require.Equal(t, uint64(2), snmp.Inserts.Load())

		const Val = 100

		require.False(t, subChan.IsDone())
		require.NoError(t, ms.Publish("a", Val))
		require.True(t, subChan.IsDone())

		snmp = ms.Snmp()

		require.Equal(t, uint64(1), snmp.Matches.Load())
		require.Equal(t, uint64(1), snmp.Count.Load())
		require.Equal(t, uint64(1), snmp.Removes.Load())

		require.Equal(t, Val, cbRet)
		require.Equal(t, Val, (<-retChan).Val())

		require.NoError(t, ms.Publish("a", Val*2))
		require.Equal(t, Val*2, cbRet)
		select {
		case v := <-retChan:
			require.Fail(t, "unexpected value", v.Val())
		default:
		}
		snmp = ms.Snmp()
		require.Equal(t, uint64(2), snmp.Matches.Load())
	})

	t.Run("Remove", func(t *testing.T) {
		ms := NewMultiSublist(4)
		const SubNum = 1000
		var allSubs = make([]*Subscription, 0, SubNum)

		// generate random subject
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		emptyCb := func(p Param[int]) {}

		for i := 0; i < SubNum; i++ {
			sub := NewSubs(fmt.Sprintf("%d.%d", i, r.Intn(SubNum)),
				Any[int](emptyCb))
			require.NoError(t, ms.Subscribe(sub))
			allSubs = append(allSubs, sub)
		}
		snmp := ms.Snmp()
		require.Equal(t, uint64(SubNum), snmp.Count.Load())

		require.NoError(t, ms.Remove(allSubs[0]))
		snmp = ms.Snmp()
		require.Equal(t, uint64(SubNum-1), snmp.Count.Load())
		require.Equal(t, uint64(1), snmp.Removes.Load())

		removeErr := ms.Remove(allSubs...)
		require.ErrorIs(t, removeErr, ErrNotFound)
		snmp = ms.Snmp()
		require.Equal(t, uint64(0), snmp.Count.Load())
		require.Equal(t, uint64(SubNum), snmp.Removes.Load())
	})
}

func TestParallel(t *testing.T) {
	var wg sync.WaitGroup
	const (
		Parallel = 1000
		Multi    = 10
	)
	wg.Add(Parallel)

	ml := NewMultiSublist(5)

	var total atomic.Int64
	emptyCb := func(p Param[int]) {
		total.Add(int64(p.Val()))
	}

	var randPool = sync.Pool{
		New: func() interface{} {
			return rand.New(rand.NewSource(time.Now().UnixNano()))
		},
	}

	var subsChan = make(chan *Subscription, Parallel)
	var allSubs = make([]*Subscription, 0, Parallel)
	var addEnd = make(chan struct{})

	var chanWait sync.WaitGroup

	var receive = make(chan Param[int], Parallel)
	chanWait.Add(1)
	go func() {
		defer chanWait.Done()
		for p := range receive {
			emptyCb(p)
		}
	}()

	for i := 0; i < Parallel; i++ {
		go func(i int) {
			defer wg.Done()
			r := randPool.Get().(*rand.Rand)
			defer randPool.Put(r)
			var sub ISubscribe
			if r.Intn(2) == 0 {
				sub = Callback(emptyCb)
			} else {
				sub = Chan(receive)
			}
			var subj []string
			// for i := 0; i < 3; i++ {
			subj = append(subj, fmt.Sprintf("%x", i))
			// }
			subs := NewSubs(strings.Join(subj, TSep), sub)
			require.NoError(t, ml.Subscribe(subs))
			subsChan <- subs
		}(i)
	}
	go func() {
		for sub := range subsChan {
			allSubs = append(allSubs, sub)
		}
		close(addEnd)
	}()
	wg.Wait()
	close(subsChan)
	<-addEnd

	snmp := ml.Snmp()
	require.Equal(t, uint64(Parallel), snmp.Count.Load())

	wg.Add(Parallel * Multi)
	for i := 0; i < Parallel*Multi; i++ {
		go func() {
			defer wg.Done()
			r := randPool.Get().(*rand.Rand)
			defer randPool.Put(r)
			sub := allSubs[r.Intn(len(allSubs))]
			require.NoError(t, ml.Publish(sub.Subject(), 1))
		}()
	}
	wg.Wait()

	time.Sleep(time.Second)
	close(receive)
	chanWait.Wait()
	require.Equal(t, int64(Parallel*Multi), total.Load())
}

func BenchmarkMS(b *testing.B) {
	var subj = "aaaaa.bbbbb.ccccc.ddddd.eeeee.fffff.ggggg.hhhhh"
	var emptyCb = func(p Param[int]) {}
	b.Run("sub", func(b *testing.B) {
		ms := NewMultiSublist(8)
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				ms.Subscribe(NewSubs(subj, Callback(emptyCb)))
			}
		})
	})
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("pub", func(b *testing.B) {
		ms := NewMultiSublist(8)
		sub := NewSubs(subj, Callback(emptyCb))
		ms.Subscribe(sub)
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				ms.Publish(subj, 1)
			}
		})
	})
	b.ReportAllocs()
}
