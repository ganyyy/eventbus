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

		s := CallbackSubs("a", func(p Msg[int]) {
			ret.Add(int64(p.Val()))
		}, Once())
		var wg sync.WaitGroup
		const P = 10
		wg.Add(P)

		calc := func() {
			defer wg.Done()
			s.call(1, nil)
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

	t.Run("Request", func(t *testing.T) {
		sublist := NewMultiBus(4)

		const subject = "a"

		var count int
		sub1 := CallbackSubs(subject, func(v Msg[int]) {
			count++
			require.Equal(t, v.Reply(v.Val()+1), count == 1)
		})
		sub2 := CallbackSubs(subject, func(v Msg[int]) {
			count++
			require.Equal(t, v.Reply(v.Val()+1), count == 1)
		})

		require.NoError(t, sublist.Subscribe(sub1))
		require.NoError(t, sublist.Subscribe(sub2))

		var reply = NewReply[int]()
		require.NoError(t, sublist.Request(subject, 1, reply))

		resp, valid := reply.Resp(context.Background())
		require.True(t, valid)
		require.Equal(t, 2, resp.Val())

		require.NoError(t, sublist.Publish(subject, 1))
		require.Equal(t, 4, count)
	})

	t.Run("RequestTimeout", func(t *testing.T) {
		sublist := NewBus()

		const subject = "a"

		sub1 := CallbackSubs(subject, func(Msg[int]) {})

		require.NoError(t, sublist.Subscribe(sub1))

		var reply = NewReply[int]()
		require.NoError(t, sublist.Request(subject, 1, reply))

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()
		resp, valid := reply.Resp(ctx)
		require.False(t, valid)
		require.Zero(t, resp.Val())
	})

	t.Run("nodes", func(t *testing.T) {
		testNodeCount(t, NewBus())
	})
}

func testNodeCount(t *testing.T, bus ISublist) {
	makeSub := func(sub string) *Subscription {
		return NewSubs(sub, nil)
	}

	a := makeSub("a")
	b := makeSub("b")
	ab := makeSub("a.b")

	require.NoError(t, bus.Subscribe(a))
	require.Equal(t, 1, bus.TotalNodes())
	require.NoError(t, bus.Subscribe(b))
	require.Equal(t, 2, bus.TotalNodes())
	require.NoError(t, bus.Subscribe(ab))
	require.Equal(t, 3, bus.TotalNodes())
	require.NoError(t, bus.Unsubscribe(a))
	require.Equal(t, 3, bus.TotalNodes())
	require.NoError(t, bus.Unsubscribe(b))
	require.Equal(t, 2, bus.TotalNodes())
	require.NoError(t, bus.Unsubscribe(ab))
	require.Equal(t, 0, bus.TotalNodes())
}

func TestMultiSublist(t *testing.T) {

	t.Run("Subscribe", func(t *testing.T) {

		emptyCallback := Any[int, func(Msg[int])](nil)
		emptyChan := Any[int, chan Msg[int]](nil)

		var ms = NewMultiBus(1)
		require.NoError(t, ms.Subscribe(NewSubs("a", emptyCallback)))
		require.NoError(t, ms.Subscribe(NewSubs("a", emptyChan)))

		require.ErrorIs(t, ms.Subscribe(NewSubs("", emptyCallback)), ErrInvalidSubject)
		require.ErrorIs(t, ms.Subscribe(NewSubs("a.", emptyCallback)), ErrInvalidSubject)
	})

	t.Run("Publish", func(t *testing.T) {
		var ms = NewMultiBus(1)

		var cbRet int
		var subCB = NewSubs("a", Callback(func(p Msg[int]) {
			cbRet = p.Val()
		}))

		var retChan = make(chan Msg[int], 1)
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

		retChan, subChan := ChanSubs[int]("a", 1)

		var ms = NewMultiBus(1)
		require.NoError(t, ms.Subscribe(subChan))
		require.NoError(t, ms.Publish("a", 1))
		err := ms.Publish("a", 1)
		require.ErrorIs(t, err, ErrSlowConsumer)
		t.Logf("error: %v", err)
		ret := <-retChan
		require.Equal(t, 1, ret.Val())
	})

	t.Run("Remove", func(t *testing.T) {
		ms := NewMultiBus(4)
		const SubNum = 1000
		var allSubs = make([]*Subscription, 0, SubNum)

		// generate random subject
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		emptyCb := func(p Msg[int]) {}

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

	// t.Run("nodes", func(t *testing.T) {
	// 	testNodeCount(t, NewMultiBus(4))
	// })
}

func TestQueue(t *testing.T) {
	t.Run("Queue", func(t *testing.T) {
		var ms = NewMultiBus(4)
		const subj = "a"

		var ret atomic.Int64
		mkQueueSub := func(queue string) *Subscription {
			return NewSubs(subj, Callback(func(p Msg[int]) {
				ret.Add(int64(p.Val()))
			}), Queue(queue))
		}

		const N = 10
		allQueue := []string{"a", "b", "c"}
		var queueSubs, subs []*Subscription
		for _, q := range append([]string{""}, allQueue...) {
			for range N {
				sub := mkQueueSub(q)
				if sub.isQueue() {
					queueSubs = append(queueSubs, sub)
				} else {
					subs = append(subs, sub)
				}
				require.NoError(t, ms.Subscribe(sub))
			}
		}
		require.NoError(t, ms.Publish(subj, 1))
		// the total ret is N + 3
		require.Equal(t, int64(N+len(allQueue)), ret.Load())
		// the total subs is N * 4
		require.Equal(t, uint64(N*(len(allQueue)+1)), ms.SnmpInfo().Count.Load())

		shuffleSlices(queueSubs)

		require.ErrorIs(t, ErrNotFound, ms.Unsubscribe(mkQueueSub("d")))
		require.ErrorIs(t, ErrNotFound, ms.Unsubscribe(mkQueueSub("a")))

		require.Equal(t, int(1), ms.TotalNodes())

		for _, sub := range subs {
			require.NoError(t, ms.Unsubscribe(sub))
		}
		require.Equal(t, int(1), ms.TotalNodes())

		for _, q := range queueSubs {
			require.NoError(t, ms.Unsubscribe(q))
		}
		require.Equal(t, int(0), ms.TotalNodes())
	})

	t.Run("Probability", func(t *testing.T) {
		const N = 100
		var count [N]atomic.Uint64
		var stat [N]uint64

		var bus = NewMultiBus(4)

		const (
			subj  = "a"
			queue = "q"
		)

		mkQueue := func(idx int) *Subscription {
			return NewSubs(subj, Callback(func(p Msg[int]) {
				count[idx].Add(1)
			}), Queue(queue))
		}

		for i := range N {
			require.NoError(t, bus.Subscribe(mkQueue(i)))
		}

		var wg sync.WaitGroup
		wg.Add(N)

		for i := range N {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < N; j++ {
					require.NoError(t, bus.Publish(subj, 1))
				}
			}(i)
		}
		wg.Wait()

		for i := range N {
			stat[i] = count[i].Load()
		}

		for i := 0; i < N; i += 10 {
			t.Logf("%2d-%3d %v", i, i+10, stat[i:i+10])
		}
	})
}

func TestWildcard(t *testing.T) {
	t.Run("subs", func(t *testing.T) {
		var w1Count, w2Count, w3Count, fCount atomic.Int64
		wSubs1 := CallbackSubs("time.*.east", func(m Msg[int]) {
			w1Count.Add(int64(m.Val()))
		})
		wSubs2 := CallbackSubs("time.*.west", func(m Msg[int]) {
			w2Count.Add(int64(m.Val()))
		})
		wSubs3 := CallbackSubs("time.*", func(m Msg[int]) {
			w3Count.Add(int64(m.Val()))
		})
		fSubs := CallbackSubs("time.>", func(m Msg[int]) {
			fCount.Add(int64(m.Val()))
		})

		errSub := CallbackSubs("time.>.>", func(m Msg[int]) {})

		var ms = NewBus()
		require.ErrorIs(t, ms.Subscribe(errSub), ErrInvalidSubject)
		require.NoError(t, ms.Subscribe(wSubs1))
		require.NoError(t, ms.Subscribe(wSubs2))
		require.NoError(t, ms.Subscribe(wSubs3))
		require.NoError(t, ms.Subscribe(fSubs))

		require.NoError(t, ms.Publish("time.now.east", 1))
		require.NoError(t, ms.Publish("time.now.west", 1))
		require.NoError(t, ms.Publish("time.*", 1))
		require.NoError(t, ms.Publish("time.>", 1))
		require.NoError(t, ms.Publish("time.now", 1))

		require.Equal(t, int64(1), w1Count.Load())
		require.Equal(t, int64(1), w2Count.Load())
		require.Equal(t, int64(3), w3Count.Load())
		require.Equal(t, int64(5), fCount.Load())
	})

	t.Run("unsubs", func(t *testing.T) {
		var w1Count, fCount atomic.Int64
		wSubs1 := CallbackSubs("time.*.east", func(m Msg[int]) {
			w1Count.Add(int64(m.Val()))
		})
		fSubs := CallbackSubs("time.>", func(m Msg[int]) {
			fCount.Add(int64(m.Val()))
		})

		errSubs1 := CallbackSubs("time.>.>", func(m Msg[int]) {})
		errSubs2 := CallbackSubs("time.time.time", func(m Msg[int]) {})

		var ms = NewBus()
		require.NoError(t, ms.Subscribe(wSubs1))
		require.NoError(t, ms.Subscribe(fSubs))

		require.ErrorIs(t, ms.Unsubscribe(errSubs1), ErrInvalidSubject)
		require.ErrorIs(t, ms.Unsubscribe(errSubs2), ErrNotFound)

		require.NoError(t, ms.Publish("time.now.east", 1))
		require.NoError(t, ms.Publish("time.now", 1))

		require.Equal(t, int64(1), w1Count.Load())
		require.Equal(t, int64(2), fCount.Load())

		require.NoError(t, ms.Unsubscribe(wSubs1))
		require.NoError(t, ms.Publish("time.now.east", 1))
		require.Equal(t, int64(1), w1Count.Load())
		require.Equal(t, int64(3), fCount.Load())

		require.Equal(t, int(2), ms.TotalNodes())

		require.NoError(t, ms.Unsubscribe(fSubs))
		require.NoError(t, ms.Publish("time.now", 1))
		require.Equal(t, int64(1), w1Count.Load())
		require.Equal(t, int64(3), fCount.Load())

		require.Equal(t, int(0), ms.TotalNodes())

	})

	t.Run("queue", func(t *testing.T) {
		var w1Count, fCount, count atomic.Int64
		wSubs1 := CallbackSubs("time.*.east", func(m Msg[int]) {
			t.Logf("call in time.*.east")
			w1Count.Add(int64(m.Val()))
		}, Queue("a"))
		fSubs := CallbackSubs("time.>", func(m Msg[int]) {
			t.Logf("call in time.>")
			fCount.Add(int64(m.Val()))
		}, Queue("a"))
		sub := CallbackSubs("time.now.east", func(m Msg[int]) {
			t.Logf("call in time.now.east")
			count.Add(int64(m.Val()))
		}, Queue("a"))

		var ms = NewBus()
		require.NoError(t, ms.Subscribe(wSubs1))
		require.NoError(t, ms.Subscribe(fSubs))
		require.NoError(t, ms.Subscribe(sub))

		require.NoError(t, ms.Publish("time.now.east", 1))

		require.Equal(t, int64(1), w1Count.Load()+fCount.Load()+count.Load())

		require.NoError(t, ms.Publish("time.now", 1))
		require.Equal(t, int64(2), w1Count.Load()+fCount.Load()+count.Load())
		require.True(t, fCount.Load() >= 1)

	})

	t.Run("NotSupport", func(t *testing.T) {
		var bus = NewMultiBus(4)
		require.ErrorIs(t, bus.Subscribe(NewSubs("time.>", nil)), ErrNotSupport)
		require.ErrorIs(t, bus.Subscribe(NewSubs("time.*", nil)), ErrNotSupport)
		require.ErrorIs(t, bus.Unsubscribe(NewSubs("time.*", nil)), ErrNotFound)
		require.NoError(t, bus.Subscribe(NewSubs("time.>*", nil)))
		require.NoError(t, bus.Subscribe(NewSubs("time.*>", nil)))
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
		emptyCb := func(p Msg[int]) {
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

		var receive = make(chan Msg[int], Parallel*Multi)

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
	var emptyCb = func(p Msg[int]) {}
	var emptyChan = make(chan Msg[int], 1)

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
