package main

import (
	"sync"
	"sync/atomic"

	"github.com/ganyyy/eventbus"
)

func main() {

	var bus = eventbus.NewMultiBus(4)

	const (
		Topic = "test.1"
		Num   = 100
	)

	var total1, total2 atomic.Int64
	var subCallback = eventbus.NewSubs(
		Topic,
		eventbus.Callback(func(p eventbus.Var[int]) {
			total1.Add(int64(p.Val()))
		}),
	)

	var notifyChannel = make(chan eventbus.Var[int], Num)
	var subChan = eventbus.NewSubs(
		Topic,
		eventbus.Chan(notifyChannel),
	)
	var stopNotify = make(chan struct{})

	go func() {
		for val := range notifyChannel {
			total2.Add(int64(val.Val()))
		}
		close(stopNotify)
	}()

	bus.Subscribe(subCallback)
	bus.Subscribe(subChan)

	defer bus.Unsubscribe(subChan)
	defer bus.Unsubscribe(subCallback)

	var wg sync.WaitGroup
	wg.Add(Num)

	for i := range Num {
		go func(i int) {
			bus.Publish(Topic, i)
			wg.Done()
		}(i)
	}

	wg.Wait()
	close(notifyChannel)
	<-stopNotify

	println(total1.Load(), total2.Load())
}
