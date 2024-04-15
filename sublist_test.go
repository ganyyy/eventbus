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
	return NewSubs(sub, nil)
}

func TestSublist(t *testing.T) {

	t.Run("subs", func(t *testing.T) {
		var ret atomic.Int64

		var s = NewSubs("a", Callback(func(p Var[int]) {
			ret.Add(int64(p.Val()))
		}), Once())
		var wg sync.WaitGroup
		const P = 10
		wg.Add(P)

		calc := func() {
			defer wg.Done()
			s.call(1)
		}

		for i := 0; i < P; i++ {
			go calc()
		}
		wg.Wait()

		require.Equal(t, int64(1), ret.Load())

		s = NewSubs("a", nil)
		s.stop()
		require.False(t, s.canCall())
	})

	t.Run("SublistNil", func(t *testing.T) {
		var s *Sublist
		require.ErrorIs(t, s.Subscribe(Subject("a")), ErrSublistNil)
		require.ErrorIs(t, s.Unsubscribe(Subject("a")), ErrSublistNil)
		require.ErrorIs(t, s.UnsubscribeBatch(nil), ErrSublistNil)
		require.ErrorIs(t, s.Publish("a", 1), ErrSublistNil)
	})

	defaultSublistWithSubs := func() (*Sublist, []*Subscription) {
		// Create a new sublist
		sublist := NewBus()
		// Insert some subscriptions
		subs := []*Subscription{
			Subject("a"),
			Subject("a.b"),
			Subject("a.b.c"),
		}
		for _, sub := range subs {
			sublist.Subscribe(sub)
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

		require.NoError(t, sublist.Subscribe(Subject("a")))
		require.Error(t, sublist.Subscribe(Subject("a.")))
		require.Error(t, sublist.Subscribe(Subject("")))

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
		var sublist = NewBus()
		const TOTAL = PListCacheMin * 2
		var subSet = NewSet[*Subscription](0)
		for i := 0; i < TOTAL; i++ {
			sub := Subject("a")
			subSet.Add(sub)
			sublist.Subscribe(sub)
		}
		require.Equal(t, TOTAL, subSet.Len())
		require.Equal(t, uint64(TOTAL), sublist.Count.Load())

		ret := sublist.match("a")
		require.Equal(t, TOTAL, len(ret.psubs))
		for _, sub := range ret.psubs {
			require.True(t, subSet.Contains(sub))
		}
		require.Equal(t, uint64(1), sublist.Matches.Load())

		require.NoError(t, sublist.Unsubscribe(ret.psubs[0]))
	})

	t.Run("Remove", func(t *testing.T) {
		var sublist, subs = defaultSublistWithSubs()
		require.Equal(t, uint64(3), sublist.Count.Load())

		require.ErrorIs(t, sublist.Unsubscribe(Subject("a.")), ErrInvalidSubject)
		require.ErrorIs(t, sublist.Unsubscribe(Subject("a")), ErrNotFound)
		require.NoError(t, sublist.Unsubscribe(subs[0]))
		require.Equal(t, uint64(2), sublist.Count.Load())
		require.Equal(t, uint64(1), sublist.Removes.Load())

		require.ErrorIs(t, sublist.Unsubscribe(Subject("a.c")), ErrNotFound)
		require.ErrorIs(t, sublist.UnsubscribeBatch(subs[:1]), ErrNotFound)

		require.NoError(t, sublist.UnsubscribeBatch(subs[1:]))
		require.Equal(t, uint64(0), sublist.Count.Load())
		require.Equal(t, uint64(3), sublist.Removes.Load())

		require.ErrorIs(t, sublist.UnsubscribeBatch(subs), ErrNotFound)

		err := sublist.UnsubscribeBatch(append(subs, Subject("a.")))
		require.ErrorIs(t, err, ErrInvalidSubject)
		require.ErrorIs(t, err, ErrNotFound)

		require.ErrorIs(t, sublist.UnsubscribeBatch([]*Subscription{
			Subject(""),
			Subject("a."),
			Subject(".a"),
			Subject("a.b."),
		}), ErrInvalidSubject)

		require.NoError(t, sublist.UnsubscribeBatch(nil))
	})

	t.Run("RemoveBatch", func(t *testing.T) {
		sublist := NewBus()

		const subject = "a"
		const NUM = 200
		var allSubject = make([]*Subscription, 0, NUM)
		for i := 0; i < NUM; i++ {
			sub := Subject(subject)
			allSubject = append(allSubject, sub)
			require.NoError(t, sublist.Subscribe(sub))
		}
		require.Equal(t, uint64(NUM), sublist.Count.Load())

		require.NoError(t, sublist.UnsubscribeBatch(allSubject))
		require.Equal(t, uint64(0), sublist.Count.Load())
		require.Equal(t, uint64(NUM), sublist.Removes.Load())

	})
}

func TestMultiSublist(t *testing.T) {

	t.Run("Subscribe", func(t *testing.T) {

		emptyCallback := Any[int, func(Var[int])](nil)
		emptyChan := Any[int, chan Var[int]](nil)

		var ms = NewMultiBus(1)
		require.NoError(t, ms.Subscribe(NewSubs("a", emptyCallback)))
		require.NoError(t, ms.Subscribe(NewSubs("a", emptyChan)))

		require.ErrorIs(t, ms.Subscribe(NewSubs("", emptyCallback)), ErrInvalidSubject)
		require.ErrorIs(t, ms.Subscribe(NewSubs("a.", emptyCallback)), ErrInvalidSubject)
	})

	t.Run("Publish", func(t *testing.T) {
		var ms = NewMultiBus(1)

		var cbRet int
		var subCB = NewSubs("a", Callback(func(p Var[int]) {
			cbRet = p.Val()
		}))

		var retChan = make(chan Var[int], 1)
		var subChan = NewSubs("a", Chan(retChan), Once())

		require.NoError(t, ms.Subscribe(subCB))
		require.NoError(t, ms.Subscribe(subChan))

		snmp := ms.SnmpInfo()
		require.Equal(t, uint64(2), snmp.Count.Load())
		require.Equal(t, uint64(2), snmp.Inserts.Load())

		const Val = 100

		require.False(t, subChan.isDone())
		require.NoError(t, ms.Publish("a", Val))
		require.True(t, subChan.isDone())

		snmp = ms.SnmpInfo()

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
		snmp = ms.SnmpInfo()
		require.Equal(t, uint64(2), snmp.Matches.Load())

	})

	t.Run("slow", func(t *testing.T) {
		var retChan = make(chan Var[int], 1)
		subChan := NewSubs("a", Chan(retChan))
		var ms = NewMultiBus(1)
		require.NoError(t, ms.Subscribe(subChan))
		require.NoError(t, ms.Publish("a", 1))
		err := ms.Publish("a", 1)
		require.ErrorIs(t, err, ErrSlowConsumer)
		t.Logf("error: %v", err)
	})

	t.Run("Remove", func(t *testing.T) {
		ms := NewMultiBus(4)
		const SubNum = 1000
		var allSubs = make([]*Subscription, 0, SubNum)

		// generate random subject
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		emptyCb := func(p Var[int]) {}

		for i := 0; i < SubNum; i++ {
			sub := NewSubs(fmt.Sprintf("%d.%d", i, r.Intn(SubNum)),
				Any[int](emptyCb))
			require.NoError(t, ms.Subscribe(sub))
			allSubs = append(allSubs, sub)
		}
		snmp := ms.SnmpInfo()
		require.Equal(t, uint64(SubNum), snmp.Count.Load())

		require.NoError(t, ms.Unsubscribe(allSubs[0]))
		snmp = ms.SnmpInfo()
		require.Equal(t, uint64(SubNum-1), snmp.Count.Load())
		require.Equal(t, uint64(1), snmp.Removes.Load())

		removeErr := ms.UnsubscribeBatch(allSubs)
		require.ErrorIs(t, removeErr, ErrNotFound)
		snmp = ms.SnmpInfo()
		require.Equal(t, uint64(0), snmp.Count.Load())
		require.Equal(t, uint64(SubNum), snmp.Removes.Load())
	})
}

func TestParallel(t *testing.T) {
	var tt = func(t *testing.T) {
		var wg sync.WaitGroup
		const (
			Parallel = 1000
			Multi    = 5
		)
		wg.Add(Parallel)

		ml := NewMultiBus(5)

		var total atomic.Int64
		emptyCb := func(p Var[int]) {
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

		var receive = make(chan Var[int], Parallel*Multi)

		for i := 0; i < Parallel; i++ {
			go func(i int) {
				defer wg.Done()
				r := randPool.Get().(*rand.Rand)
				defer randPool.Put(r)
				var sub ICall
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

		snmp := ml.SnmpInfo()
		require.Equal(t, uint64(Parallel), snmp.Count.Load())

		chanWait.Add(1)
		go func() {
			defer chanWait.Done()
			for p := range receive {
				emptyCb(p)
			}
		}()

		wg.Add(Parallel)
		for i := 0; i < Parallel; i++ {
			go func() {
				defer wg.Done()
				r := randPool.Get().(*rand.Rand)
				defer randPool.Put(r)
				for j := 0; j < Multi; j++ {
					sub := allSubs[r.Intn(len(allSubs))]
					require.NoError(t, ml.Publish(sub.Subject(), 1))
				}
			}()
		}
		wg.Wait()
		close(receive)
		chanWait.Wait()

		require.Zero(t, len(receive))
		require.Equal(t, int64(Parallel*Multi), total.Load())
	}

	for i := 0; i < 100; i++ {
		tt(t)
	}
}

func BenchmarkMS(b *testing.B) {
	var subj = "aaaaa.bbbbb.ccccc.ddddd.eeeee.fffff.ggggg.hhhhh"
	var emptyCb = func(p Var[int]) {}
	var emptyChan = make(chan Var[int], 1)

	_ = emptyCb
	_ = emptyChan

	b.Run("sub", func(b *testing.B) {
		ms := NewMultiBus(8)
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				ms.Subscribe(NewSubs(subj, Callback(emptyCb)))
			}
		})
	})
	b.ReportAllocs()
	b.ResetTimer()

	const ElementNum = 100

	b.Run("pub cb", func(b *testing.B) {
		ms := NewMultiBus(8)
		for i := 0; i < ElementNum; i++ {
			ms.Subscribe(NewSubs(subj, Callback(emptyCb)))
		}
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				ms.Publish(subj, 1)
			}
		})
	})
	b.ReportAllocs()
	b.ResetTimer()

	b.Run("pub chan", func(b *testing.B) {
		ms := NewMultiBus(8)
		for i := 0; i < ElementNum; i++ {
			ms.Subscribe(NewSubs(subj, Chan(emptyChan)))
		}
		b.RunParallel(func(p *testing.PB) {
			for p.Next() {
				ms.Publish(subj, 1)
			}
		})
	})
	b.ReportAllocs()
}
