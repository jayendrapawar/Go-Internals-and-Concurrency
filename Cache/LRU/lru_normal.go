package main

// NormalLRU is a non-thread-safe LRU cache.
// Get and Put both run in O(1) time.
type NormalLRU struct {
	capacity int
	items    map[int]*normalNode
	head     *normalNode
	tail     *normalNode
}

type normalNode struct {
	key, value int
	prev, next *normalNode
}

func NewNormalLRU(capacity int) *NormalLRU {
	l := &NormalLRU{
		capacity: capacity,
		items:    make(map[int]*normalNode),
		head:     &normalNode{},
		tail:     &normalNode{},
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}

func (l *NormalLRU) Get(key int) (int, bool) {
	node, ok := l.items[key]
	if !ok {
		return 0, false
	}
	l.moveToFront(node)
	return node.value, true
}

func (l *NormalLRU) Put(key, value int) {
	if l.capacity <= 0 {
		return
	}
	if node, ok := l.items[key]; ok {
		node.value = value
		l.moveToFront(node)
		return
	}
	if len(l.items) == l.capacity {
		lru := l.tail.prev
		l.remove(lru)
		delete(l.items, lru.key)
	}
	node := &normalNode{key: key, value: value}
	l.items[key] = node
	l.addFront(node)
}

func (l *NormalLRU) moveToFront(node *normalNode) {
	l.remove(node)
	l.addFront(node)
}

func (l *NormalLRU) addFront(node *normalNode) {
	node.prev = l.head
	node.next = l.head.next
	l.head.next.prev = node
	l.head.next = node
}

func (l *NormalLRU) remove(node *normalNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}
