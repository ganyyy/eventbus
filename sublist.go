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
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

const (
	TSep  = "."
	BtSep = '.'
)

const (
	PListCacheMin  = 128 // build a quick cache when the number of subscriptions is greater than this value
	StackCacheSize = 32  // normal stack cache size
)

var sublistResultPool = sync.Pool{
	New: func() any {
		return &sublistResult{}
	},
}

func getResult() *sublistResult {
	return sublistResultPool.Get().(*sublistResult)
}

func putResult(r *sublistResult) {
	clear(r.psubs)
	r.psubs = r.psubs[:0]
	sublistResultPool.Put(r)
}

type sublistResult struct {
	psubs []*Subscription
}

var emptyResult = &sublistResult{}

type Snmp struct {
	Matches atomic.Uint64
	Count   atomic.Uint64
	Inserts atomic.Uint64
	Removes atomic.Uint64
}

type Sublist struct {
	lock sync.RWMutex

	Snmp

	genId atomic.Uint64 // sublist generation id. changed on insert/remove
	root  *Level
}

// NewSublist
func NewSublist() *Sublist {
	return &Sublist{
		root: NewLevel(),
	}
}

type Subscription struct {
	subject string // topic
	status  atomic.Int32
	sub     ICall

	subsOption
}

// NewSubs
func NewSubs(subject string, call ICall, opts ...Opt) *Subscription {
	var subs = Subscription{
		subject: subject,
		sub:     call,
	}
	subs.apply(opts...)
	return &subs
}

// Subject
func (s *Subscription) Subject() string {
	if s == nil {
		return ""
	}
	return s.subject
}

// isOnce returns true if the subscribe is once
func (s *Subscription) isOnce() bool { return s.once }

// canCall returns true if the subscribe can be called
func (s *Subscription) canCall() bool {
	// if the subscribe is done, return false
	// if the subscribe is once and executed, return false

	if s.isDone() {
		return false
	}
	if !s.isOnce() {
		return true
	}

	return s.status.CompareAndSwap(
		statusInit, statusExecuted,
	)

}

// stop stops the subscribe
func (s *Subscription) stop() {
	s.status.Store(statusClosed)
}

// isDone returns true if the subscribe is closed
func (s *Subscription) isDone() bool {
	return s.status.Load() == statusClosed
}

// split splits the subject into tokens.
func (s *Subscription) split(cache []string) ([]string, bool) {
	subject := s.Subject()
	return SplitSubject(subject, cache)
}

// call
func (s *Subscription) call(param any) bool {
	if !s.canCall() {
		return false
	}
	return s.sub.run(param)
}

// SplitSubject splits the subject into tokens.
func SplitSubject(subject string, cache []string) ([]string, bool) {
	var start int
	for i := 0; i < len(subject); i++ {
		if subject[i] == BtSep {
			if start == i {
				return nil, false
			}
			cache = append(cache, subject[start:i])
			start = i + 1
		}
	}
	return append(cache, subject[start:]), start < len(subject)
}

// ValidSubject returns true if the subject is valid.
func ValidSubject(subject string) bool {
	var start int
	for i := 0; i < len(subject); i++ {
		if subject[i] == BtSep {
			if start == i {
				return false
			}
			start = i + 1
		}
	}
	return start < len(subject)
}

// Insert adds the subscription into the sublist.
func (s *Sublist) Insert(sub *Subscription) error {
	if s == nil {
		return ErrSublistNil
	}

	var cache [StackCacheSize]string
	tokens, valid := sub.split(cache[:0])
	if !valid {
		return ErrInvalidSubject
	}

	var n *Node // the node to insert the subscription into
	s.lock.Lock()

	level := s.root

	for _, token := range tokens {
		n = level.Nodes[token]
		if n == nil {
			n = NewNode()
			level.Nodes[token] = n
		}

		if n.Next == nil {
			n.Next = NewLevel()
		}

		level = n.Next
	}

	n.Psubs.Add(sub)

	if n.Plist != nil {
		// when the number of subscriptions is greater than the minimum quick cache value,
		// add the subscription to the quick cache
		n.Plist = append(n.Plist, sub)
	} else if n.Psubs.Len() > PListCacheMin {
		// build a quick cache when the number of subscriptions
		// is greater than the minimum quick cache value
		n.Plist = n.Psubs.AppendToSlice(nil)
	}

	s.Count.Add(1)
	s.Inserts.Add(1)

	s.genId.Add(1)

	s.lock.Unlock()
	return nil
}

// removeFromNodeInLock removes the subscription from the node.
func (s *Sublist) removeFromNodeInLock(n *Node, sub *Subscription) (found, last bool) {
	if n == nil {
		return false, true
	}
	found = n.Psubs.Contains(sub)
	if !found {
		return
	}
	n.Psubs.Remove(sub)
	if n.Plist != nil {
		n.Plist = nil
	}
	last = n.IsEmpty()
	return
}

// Publish
func (s *Sublist) Publish(subject string, param any) (err error) {
	if s == nil {
		return ErrSublistNil
	}
	if !ValidSubject(subject) {
		return ErrInvalidSubject
	}
	var slowConsumeCount int
	ret := s.match(subject)
	var removeOnces = ret.psubs[:0]
	for _, sub := range ret.psubs {
		success := sub.call(param)
		isOnce := sub.isOnce()
		if success && isOnce {
			removeOnces = append(removeOnces, sub)
		} else if !success && !isOnce {
			if !sub.isDone() {
				// only in channel mode
				slowConsumeCount++
			}
		}
	}
	if len(removeOnces) > 0 {
		s.RemoveBatch(removeOnces)
	}
	putResult(ret)
	if slowConsumeCount > 0 {
		err = slowConsumerErr{count: slowConsumeCount}
	}
	return
}

func (s *Sublist) Remove(sub *Subscription) error {
	if s == nil {
		return ErrSublistNil
	}
	return s.remove(sub, true, true)
}

// RemoveBatch
func (s *Sublist) RemoveBatch(subs []*Subscription) error {
	if s == nil {
		return ErrSublistNil
	}
	if len(subs) == 0 {
		return nil
	}
	if len(subs) == 1 {
		return s.Remove(subs[0])
	}

	// split the subscriptions into batches

	var errorCache [StackCacheSize]error
	var allErrors = errorCache[:0]

	var validSubs = subs[:0]

	for _, sub := range subs {
		if ValidSubject(sub.Subject()) {
			validSubs = append(validSubs, sub)
		} else {
			allErrors = append(allErrors, &subjectError{
				subject: sub.Subject(),
				err:     ErrInvalidSubject,
			})
		}
	}

	if len(validSubs) == 0 {
		return errors.Join(allErrors...)
	}

	subs = validSubs

	// avoid too many locks
	const BatchNum = 32

	var batchCache [StackCacheSize / 2][]*Subscription
	var batch = batchCache[:0]

	for len(subs) > BatchNum {
		batch = append(batch, subs[:BatchNum])
		subs = subs[BatchNum:]
	}
	if len(subs) > 0 {
		batch = append(batch, subs)
	}

	for _, subs := range batch {
		var errCnt int
		s.lock.Lock()
		for _, sub := range subs {
			if err := s.remove(sub, false, false); err != nil {
				allErrors = append(allErrors, &subjectError{
					subject: sub.Subject(),
					err:     err,
				})
				errCnt++
			}
		}
		if errCnt != len(subs) {
			// maybe some subscriptions are not found
			// but some subscriptions are removed successfully
			// so we need to update the generation id
			s.genId.Add(1)
		}
		s.lock.Unlock()
	}

	return errors.Join(allErrors...)
}

// remove removes the subscription from the sublist.
func (s *Sublist) remove(sub *Subscription, lock bool, updateGen bool) error {
	var cache [StackCacheSize]string
	tokens, valid := sub.split(cache[:0])
	if !valid {
		return ErrInvalidSubject
	}

	var n *Node
	if lock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	level := s.root

	var levelCache [StackCacheSize]LevelCache
	var levels = levelCache[:0]

	for _, token := range tokens {
		n = level.Nodes[token]
		if n != nil {
			levels = append(levels, LevelCache{
				Level: level,
				Node:  n,
				Topic: token,
			})
			level = n.Next
		} else {
			// 找不到, 直接返回
			return ErrNotFound
		}
	}

	removed, _ := s.removeFromNodeInLock(n, sub)
	if !removed {
		return ErrNotFound
	}

	sub.stop()

	s.Count.Add(^uint64(0))
	s.Removes.Add(1)

	if updateGen {
		s.genId.Add(1)
	}

	for idx := len(levels) - 1; idx >= 0; idx-- {
		lv, node, token := levels[idx].Level, levels[idx].Node, levels[idx].Topic
		if node.IsEmpty() {
			lv.PruneNode(token)
		}
	}
	return nil
}

// match returns a list of subscriptions that match the subject.
func (s *Sublist) match(subject string) *sublistResult {
	if s == nil {
		return emptyResult
	}

	var cache [StackCacheSize]string
	tokens, valid := SplitSubject(subject, cache[:0])
	if !valid {
		return emptyResult
	}

	s.Matches.Add(1)
	var ret = getResult()

	s.lock.RLock()
	defer s.lock.RUnlock()

	MatchLevel(s.root, tokens, ret)
	return ret
}

// add adds the subscription to the result.
func (s *sublistResult) add(node *Node) {
	if node.Plist != nil {
		s.psubs = append(s.psubs, node.Plist...)
	} else {
		s.psubs = node.Psubs.AppendToSlice(s.psubs)
	}
}

type MultiSublist struct {
	sublists []*Sublist
	seed     maphash.Seed
}

func NewMultiSublist(length uint) *MultiSublist {

	if length < 1 {
		panic("length must be greater than 0")
	}

	lists := make([]*Sublist, length)
	for i := range lists {
		lists[i] = NewSublist()
	}

	return &MultiSublist{
		sublists: lists,
		seed:     maphash.MakeSeed(),
	}
}

// Snmp
func (m *MultiSublist) Snmp() *Snmp {
	var snmp Snmp
	for _, sublist := range m.sublists {
		snmp.Count.Add(sublist.Count.Load())
		snmp.Inserts.Add(sublist.Inserts.Load())
		snmp.Removes.Add(sublist.Removes.Load())
		snmp.Matches.Add(sublist.Matches.Load())
	}
	return &snmp
}

// getSublist use map hash to get the sublist by subject.
func (m *MultiSublist) getSublist(subject string) *Sublist {
	if m == nil {
		return nil
	}
	hash := maphash.String(m.seed, subject)
	return m.sublists[hash%uint64(len(m.sublists))]
}

// Publish
func (m *MultiSublist) Publish(subject string, param any) error {
	return m.getSublist(subject).Publish(subject, param)
}

// Subscribe
func (m *MultiSublist) Subscribe(sub *Subscription) error {
	return m.getSublist(sub.Subject()).Insert(sub)
}

// Remove
func (m *MultiSublist) Remove(subs ...*Subscription) error {
	if len(subs) < 1 {
		return nil
	} else if len(subs) == 1 {
		sub := subs[0]
		return m.getSublist(sub.Subject()).Remove(sub)
	} else {
		var sublistSet = make(map[*Sublist][]*Subscription, InitNodeSubCache)
		for _, sub := range subs {
			if !ValidSubject(sub.Subject()) {
				continue
			}
			sublist := m.getSublist(sub.Subject())
			sublistSet[sublist] = append(sublistSet[sublist], sub)
		}
		var all []error
		for sublist, subs := range sublistSet {
			if err := sublist.RemoveBatch(subs); err != nil {
				all = append(all, err)
			}
		}
		return errors.Join(all...)
	}
}

// Default is the default multi-sublist.
var Default = NewMultiSublist(32)
