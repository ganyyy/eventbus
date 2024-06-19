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
	Pwc *Node // wildcard node. *
	Fwc *Node // full wildcard node. >
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
	Token string
}

func NewLevel() *Level {
	return &Level{
		Nodes: make(map[string]*Node, InitLevelNodeCache),
	}
}

// NumNodes
func (l *Level) NumNodes() int {
	return len(l.Nodes) + addNodeNum(l.Fwc) + addNodeNum(l.Pwc)
}

func addNodeNum(n *Node) int {
	if n != nil {
		return 1
	}
	return 0
}

// PruneNode
func (l *Level) PruneNode(n *Node, token string) {
	if n == nil {
		return
	}
	if n == l.Fwc {
		l.Fwc = nil
	} else if n == l.Pwc {
		l.Pwc = nil
	} else {
		delete(l.Nodes, token)
	}
}

// IsEmpty
func (n *Node) IsEmpty() bool {
	if n == nil {
		return true
	}
	if n.Psubs.Len() == 0 && len(n.Qsubs) == 0 {
		if n.Next == nil || n.Next.NumNodes() == 0 {
			return true
		}
	}
	return false
}

// MatchLevel
func MatchLevel(level *Level, tokens []string, ret *sublistResult) {
	var node *Node
	var pwn *Node
	for idx, token := range tokens {
		if level == nil {
			return
		}
		if level.Fwc != nil {
			ret.add(level.Fwc)
		}
		if pwn = level.Pwc; pwn != nil {
			// skip one token for wildcard node
			MatchLevel(pwn.Next, tokens[idx+1:], ret)
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
	if pwn != nil {
		ret.add(pwn)
	}
}
