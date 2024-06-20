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
	Pwc   = "*"
	BPwc  = '*'
	Fwc   = ">"
	BFwc  = '>'
)

const (
	PListCacheMin  = 128 // build a quick cache when the number of subscriptions is greater than this value
	StackCacheSize = 32  // normal stack cache size
)

var sublistResultPool = NewPool(
	func() *sublistResult {
		return &sublistResult{}
	},
	func(r *sublistResult) bool {
		clear(r.psubs)
		r.psubs = r.psubs[:0]
		clear(r.qsubs)
		r.qsubs = r.qsubs[:0]
		return true
	},
)

type sublistResult struct {
	psubs []*Subscription
	qsubs [][]*Subscription
}

var emptyResult = &sublistResult{}

type Snmp struct {
	Matches atomic.Uint64
	Count   atomic.Uint64
	Inserts atomic.Uint64
	Removes atomic.Uint64
}

// AddTo adds the snmp to the to snmp.
func (s *Snmp) AddTo(to *Snmp) {
	to.Matches.Add(s.Matches.Load())
	to.Count.Add(s.Count.Load())
	to.Inserts.Add(s.Inserts.Load())
	to.Removes.Add(s.Removes.Load())
}

type Sublist struct {
	lock sync.RWMutex

	Snmp

	genId atomic.Uint64 // sublist generation id. changed on insert/remove
	root  *Level
}

// NewBus
func NewBus() *Sublist {
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
func (s *Subscription) call(param any, reply replyFunc) bool {
	if !s.canCall() {
		return false
	}
	return s.sub.run(param, reply)
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

// TotalNodes returns the total number of nodes.
func (s *Sublist) TotalNodes() int {
	if s == nil {
		return 0
	}
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.root.TotalNodes()
}

// SnmpInfo
func (s *Sublist) SnmpInfo() *Snmp {
	var ret Snmp
	ret.Count.Store(s.Count.Load())
	ret.Inserts.Store(s.Inserts.Load())
	ret.Removes.Store(s.Removes.Load())
	ret.Matches.Store(s.Matches.Load())
	return &ret
}

// Subscribe adds the subscription into the sublist.
func (s *Sublist) Subscribe(sub *Subscription) error {
	if s == nil {
		return ErrSublistNil
	}

	var cache [StackCacheSize]string
	tokens, valid := sub.split(cache[:0])
	if !valid {
		return ErrInvalidSubject
	}

	var n *Node     // the node to insert the subscription into
	var hasFwc bool // whether the full wildcard is found
	s.lock.Lock()
	defer s.lock.Unlock()

	level := s.root

	for _, token := range tokens {
		lt := len(token)
		if hasFwc {
			// the full wildcard is not allowed to be followed by other tokens
			return ErrInvalidSubject
		}
		if lt > 1 {
			// this is normal token
			n = level.Nodes[token]
		} else {
			switch token[0] {
			case BPwc: // *
				n = level.Pwc
			case BFwc: // >
				n = level.Fwc
				hasFwc = true
			default:
				n = level.Nodes[token]
			}
		}

		if n == nil {
			n = NewNode()
			if lt > 1 {
				// this is normal token
				level.Nodes[token] = n
			} else {
				switch token[0] {
				case BPwc:
					level.Pwc = n
				case BFwc:
					level.Fwc = n
				default:
					level.Nodes[token] = n
				}
			}
		}

		if n.Next == nil {
			n.Next = NewLevel()
		}

		level = n.Next
	}

	if !sub.isQueue() {
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
	} else {
		if n.Qsubs == nil {
			n.Qsubs = make(map[string]ISet[*Subscription])
		}
		queueName := sub.queue
		// add the subscription to the queue set
		subs, ok := n.Qsubs[queueName]
		if !ok {
			subs = NewMixSet[*Subscription]()
			n.Qsubs[queueName] = subs
		}
		subs.Add(sub)
	}

	s.Count.Add(1)
	s.Inserts.Add(1)

	s.genId.Add(1)

	return nil
}

// removeFromNodeInLock removes the subscription from the node.
// returns found and last
// found: true if the subscription is found in the node
// last: true if remove the last subscription in the subscription list
func (s *Sublist) removeFromNodeInLock(n *Node, sub *Subscription) (found, last bool) {
	if n == nil {
		return false, true
	}
	if !sub.isQueue() {
		// process the normal subscriptions
		found = n.Psubs.Contains(sub)
		if !found {
			return
		}
		n.Psubs.Remove(sub)
		if n.Plist != nil {
			n.Plist = nil
		}
		last = n.Psubs.Len() == 0
		return
	} else {
		// process the queue subscriptions
		queueSubs, ok := n.Qsubs[sub.queue]
		if !ok {
			return
		}
		found = queueSubs.Contains(sub)
		if found {
			queueSubs.Remove(sub)
		}
		last = queueSubs.Len() == 0
		if last {
			delete(n.Qsubs, sub.queue)
		}
	}
	return
}

// Publish
func (s *Sublist) Publish(subject string, param any) (err error) {
	return s.Request(subject, param, nil)
}

// Request
func (s *Sublist) Request(subject string, param any, reply IReply) (err error) {
	if s == nil {
		return ErrSublistNil
	}
	if !ValidSubject(subject) {
		return ErrInvalidSubject
	}
	var slowConsumeCount int
	ret := s.match(subject)
	var removeOnces = ret.psubs[:0]
	var r replyFunc
	if reply != nil {
		r = reply.reply
	}

	var call = func(sub *Subscription, param any, reply replyFunc) bool {
		success := sub.call(param, reply)
		isOnce := sub.isOnce()
		if success && isOnce {
			removeOnces = append(removeOnces, sub)
		} else if !success && !isOnce {
			if !sub.isDone() {
				// only in channel mode
				slowConsumeCount++
			}
		}
		return success
	}

	for _, sub := range ret.psubs {
		call(sub, param, r)
	}
	// process the queue subscriptions
	for _, subs := range ret.qsubs {
		length := len(subs)
		if length == 0 {
			continue
		}
		// maybe should shuffle the queue subscriptions?
		// in the queue mode, only one subscription is executed
		var start = randIdx(length)
		for i := range len(subs) {
			if call(subs[(start+i)%length], param, r) {
				break
			}
		}
	}

	if len(removeOnces) > 0 {
		s.UnsubscribeBatch(removeOnces)
	}
	sublistResultPool.Put(ret)
	if slowConsumeCount > 0 {
		err = slowConsumerErr{count: slowConsumeCount}
	}
	return
}

func (s *Sublist) Unsubscribe(sub *Subscription) error {
	if s == nil {
		return ErrSublistNil
	}
	return s.remove(sub, true, true)
}

// UnsubscribeBatch
func (s *Sublist) UnsubscribeBatch(subs []*Subscription) error {
	if s == nil {
		return ErrSublistNil
	}
	if len(subs) == 0 {
		return nil
	}
	if len(subs) == 1 {
		return s.Unsubscribe(subs[0])
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
	var hasFwc bool
	if lock {
		s.lock.Lock()
		defer s.lock.Unlock()
	}

	level := s.root

	var levelCache [StackCacheSize]LevelCache
	var levels = levelCache[:0]

	for _, token := range tokens {

		lt := len(token)
		if hasFwc {
			// the full wildcard is not allowed to be followed by other tokens
			return ErrInvalidSubject
		}
		if level == nil {
			// the level is nil, the subscription is not found
			return ErrNotFound
		}
		if lt > 1 {
			n = level.Nodes[token]
		} else {
			switch token[0] {
			case BPwc:
				n = level.Pwc
			case BFwc:
				n = level.Fwc
				hasFwc = true
			default:
				n = level.Nodes[token]
			}
		}

		if n != nil {
			levels = append(levels, LevelCache{
				Level: level,
				Node:  n,
				Token: token,
			})
			level = n.Next
		} else {
			level = nil
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
		lv, node, token := levels[idx].Level, levels[idx].Node, levels[idx].Token
		if node.IsEmpty() {
			lv.PruneNode(node, token)
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
	var ret = sublistResultPool.Get()

	s.lock.RLock()
	defer s.lock.RUnlock()

	MatchLevel(s.root, tokens, ret)
	return ret
}

const (
	insertNewPos = -1
)

// findInsertQueueIndex finds the index to insert the subscription.
func (s *sublistResult) findInsertQueueIndex(queueName string) int {
	if queueName == "" {
		return insertNewPos
	}
	for idx, subs := range s.qsubs {
		if len(subs) == 0 {
			continue
		}
		if subs[0].queue == queueName {
			return idx
		}
	}
	return insertNewPos
}

// add adds the subscription to the result.
func (s *sublistResult) add(node *Node) {
	if node.Plist != nil {
		s.psubs = append(s.psubs, node.Plist...)
	} else {
		s.psubs = node.Psubs.AppendToSlice(s.psubs)
	}

	// process the queue subscriptions
	for queueName, subs := range node.Qsubs {
		if subs.Len() == 0 {
			continue
		}
		// TODO queue weight?
		if idx := s.findInsertQueueIndex(queueName); idx == insertNewPos {
			s.qsubs = append(s.qsubs, subs.AppendToSlice(nil))
		} else {
			s.qsubs[idx] = subs.AppendToSlice(s.qsubs[idx])
		}
	}
}

// MultiSublist is a multi-sublist.
// NOT-SUPPORT WILD SUBSCRIPTION.
type MultiSublist struct {
	sublists []*Sublist
	seed     maphash.Seed
}

func NewMultiBus(length uint) *MultiSublist {

	if length < 1 {
		panic("length must be greater than 0")
	}

	lists := make([]*Sublist, length)
	for i := range lists {
		lists[i] = NewBus()
	}

	return &MultiSublist{
		sublists: lists,
		seed:     maphash.MakeSeed(),
	}
}

func (m *MultiSublist) IsSubjectValid(subject string) (isWild bool, valid bool) {
	var cache [StackCacheSize]string
	var token []string
	token, valid = SplitSubject(subject, cache[:0])
	if !valid {
		return
	}
	for _, t := range token {
		if t == Pwc || t == Fwc {
			isWild = true
			return
		}
	}
	return
}

// TotalNodes returns the total number of nodes.
func (m *MultiSublist) TotalNodes() int {
	if m == nil {
		return 0
	}
	var total int
	for _, sublist := range m.sublists {
		total += sublist.TotalNodes()
	}
	return total
}

// SnmpInfo
func (m *MultiSublist) SnmpInfo() *Snmp {
	var snmp Snmp
	for _, sublist := range m.sublists {
		sublist.AddTo(&snmp)
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

// Request
func (m *MultiSublist) Request(subject string, param any, reply IReply) error {
	return m.getSublist(subject).Request(subject, param, reply)
}

// Subscribe
func (m *MultiSublist) Subscribe(sub *Subscription) error {
	if isWild, valid := m.IsSubjectValid(sub.Subject()); !valid {
		return ErrInvalidSubject
	} else if isWild {
		return ErrNotSupport
	}
	return m.getSublist(sub.Subject()).Subscribe(sub)
}

// Unsubscribe
func (m *MultiSublist) Unsubscribe(subs *Subscription) error {
	return m.getSublist(subs.Subject()).Unsubscribe(subs)
}

// UnsubscribeBatch
func (m *MultiSublist) UnsubscribeBatch(subs []*Subscription) error {
	if len(subs) < 1 {
		return nil
	} else if len(subs) == 1 {
		return m.Unsubscribe(subs[0])
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
			if err := sublist.UnsubscribeBatch(subs); err != nil {
				all = append(all, err)
			}
		}
		return errors.Join(all...)
	}
}

// Default is the default multi-sublist.
var Default = NewMultiBus(32)

type ISublist interface {
	Subscribe(sub *Subscription) error
	Publish(subject string, param any) error
	Request(subject string, param any, reply IReply) error

	Unsubscribe(sub *Subscription) error
	UnsubscribeBatch(subs []*Subscription) error

	SnmpInfo() *Snmp
	TotalNodes() int
}

var (
	_ ISublist = (*Sublist)(nil)
	_ ISublist = (*MultiSublist)(nil)
)
