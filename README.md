# eventbus
纯内存实现的发布订阅模式的事件总线，订阅逻辑仿照的nats

## 特点

1. 并发安全
2. 通过泛型提供有限的类型安全
3. 支持回调和channel两种订阅方式
4. 支持订阅者取消订阅



## TODO

- [ ] 支持queue模式
- [ ] 支持通配匹配和前向匹配
- [ ] 支持同步的Request/Reply模式

# 使用方法

> 安装

```
go get github.com/ganyyy/eventbus
```

> 使用

```go
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

```