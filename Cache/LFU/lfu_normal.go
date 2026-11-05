package main

// LFU eviction policy: least frequency first; LRU breaks frequency ties.
type LFU struct {
	capacity int
	minFreq  int
	items    map[int]*node
	lists    map[int]*list
}
type node struct {
	key, value, freq int
	prev, next       *node
}
type list struct {
	head, tail *node
	size       int
}

func newList() *list {
	h, t := &node{}, &node{}
	h.next, t.prev = t, h
	return &list{head: h, tail: t}
}

func (l *list) pushFront(n *node) {
	n.prev, n.next = l.head, l.head.next
	l.head.next.prev, l.head.next = n, n
	l.size++
}

func (l *list) remove(n *node) {
	n.prev.next, n.next.prev = n.next, n.prev
	l.size--
}

func (l *list) popBack() *node {
	if l.size == 0 {
		return nil
	}
	n := l.tail.prev
	l.remove(n)
	return n
}

func NewLFU(capacity int) *LFU {
	return &LFU{capacity: capacity, items: make(map[int]*node), lists: make(map[int]*list)}
}

func (c *LFU) Get(key int) (int, bool) {
	n, ok := c.items[key]
	if !ok {
		return 0, false
	}
	c.bump(n)
	return n.value, true
}

func (c *LFU) Put(key, value int) {
	if c.capacity <= 0 {
		return
	}
	if n, ok := c.items[key]; ok {
		n.value = value
		c.bump(n)
		return
	}
	if len(c.items) == c.capacity {
		l := c.lists[c.minFreq]
		evicted := l.popBack()
		delete(c.items, evicted.key)
		if l.size == 0 {
			delete(c.lists, c.minFreq)
		}
	}
	n := &node{key: key, value: value, freq: 1}
	if c.lists[1] == nil {
		c.lists[1] = newList()
	}
	c.lists[1].pushFront(n)
	c.items[key] = n
	c.minFreq = 1
}

func (c *LFU) bump(n *node) {
	old := n.freq
	l := c.lists[old]
	l.remove(n)
	if l.size == 0 {
		delete(c.lists, old)
		if c.minFreq == old {
			c.minFreq++
		}
	}
	n.freq++
	if c.lists[n.freq] == nil {
		c.lists[n.freq] = newList()
	}
	c.lists[n.freq].pushFront(n)
}
