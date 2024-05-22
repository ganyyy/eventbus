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

const (
	InitNodeSubCache   = 16
	InitLevelNodeCache = 16
)

type Level struct {
	Nodes map[string]*Node //
	// TODO
}

type Node struct {
	Next  *Level                         // next level
	Psubs ISet[*Subscription]            // original set
	Qsubs map[string]ISet[*Subscription] // queue set
	Plist []*Subscription                // cache list
}

func NewNode() *Node {
	return &Node{
		Psubs: NewMixSet[*Subscription](),
		// Psubs: NewSet[*Subscription](InitNodeSubCache),
	}
}

type LevelCache struct {
	Level *Level
	Node  *Node
	Topic string
}

func NewLevel() *Level {
	return &Level{
		Nodes: make(map[string]*Node, InitLevelNodeCache),
	}
}

// NumNodes
func (l *Level) NumNodes() int {
	return len(l.Nodes)
}

// PruneNode
func (l *Level) PruneNode(topic string) {
	delete(l.Nodes, topic)
}

// IsEmpty
func (n *Node) IsEmpty() bool {
	return n.Psubs.Len() == 0 && (n.Next == nil || n.Next.NumNodes() == 0)
}

// MatchLevel
func MatchLevel(level *Level, tokens []string, ret *sublistResult) {
	var node *Node
	for _, token := range tokens {
		if level == nil {
			return
		}
		node = level.Nodes[token]
		if node == nil {
			level = nil
		} else {
			level = node.Next
		}
	}
	if node != nil {
		ret.add(node)
	}
}
