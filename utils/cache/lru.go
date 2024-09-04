package cache

import (
	"container/list"
	"fmt"
)

// windowLRU 结构体用于实现一个定长的 LRU (Least Recently Used) 缓存。
// 它通过一个哈希表和一个双向链表实现，哈希表用于快速检索缓存项，
// 双向链表用于维护缓存项的使用顺序。
type windowLRU struct {
	data map[uint64]*list.Element // data 是一个哈希表，用于存储缓存项的键值对，键为缓存项的唯一标识，值为双向链表中的元素指针。
	cap  int                      // cap 表示 LRU 缓存的最大容量，即最多可以存储的缓存项数。
	list *list.List               // List我们用来记录数据的先后访问顺序，每次访问，都把本次访问的节点移动到链表中的头部。这样子整个链表就会按照近期的访问记录来排序了。
}

// storeItem 结构体用于表示存储在数据结构或数据库中的一个条目。
// 它提供了一种方式来组织和管理存储的数据，特别是在处理并发操作和数据一致性时。
type storeItem struct {
	// stage 在主缓存中的哪个区(probation 缓刑区 和 protected 主缓存区)
	stage int
	// key 是用于标识条目的唯一键值，确保每个条目都是可识别且唯一的。
	key uint64
	// conflict 是用于解决哈希冲突的键值，用于在哈希冲突时进行冲突解决。
	conflict uint64
	// value 存储了条目的实际数据内容，它可以是任何类型的值。
	value interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		list: list.New(),
	}
}

// add 方法用于向 LRU 缓存中添加一个新的存储项。
// 如果添加成功，返回的 storeItem 将是空项，evicted 为 false。
// 如果发生了替换操作，返回的 storeItem 将是被替换出的项，evicted 为 true。
func (lru *windowLRU) add(newItem storeItem) (oldItem storeItem, evicted bool) {
	// 检查 LRU 链表是否已满
	if lru.list.Len() < lru.cap {
		// 如果未满，直接将新项添加到链表头部
		lru.data[newItem.key] = lru.list.PushFront(&newItem)
		return storeItem{}, false
	}
	// 如果已满，需要根据 LRU 策略进行替换 获取链表尾部的项，这是最近最少使用的项
	//evictItem 是指向链表尾部的项的指针，
	evictItem := lru.list.Back()
	// 用指针访问其指向的值，用evictItem.Value(*storeItem) 表示我们要从接口中提取的值的类型，即 storeItem 类型的指针。
	//evictItem.Value.(*storeItem) 是类型断言的语法，它尝试将 evictItem.Value 中的值转换为 storeItem 类型的指针。
	//如果 evictItem.Value 中的实际值确实是一个 storeItem 类型的指针，那么这个类型断言将会成功，并且 item 将会被赋予这个值。
	item := evictItem.Value.(*storeItem)

	// 从哈希表中删除待替换的项
	delete(lru.data, item.key)

	// 替换 evictItem 的值为新项，这样可以重用已存在的内存空间
	oldItem, *item = *item, newItem

	// 更新哈希表，将新项的链表节点引用保存
	lru.data[item.key] = evictItem

	// 将替换过的新项移动到链表头部
	lru.list.MoveToFront(evictItem)
	return oldItem, true
}

func (lru *windowLRU) get(v *list.Element) {
	lru.list.MoveToFront(v)
}

// String 方法返回 LRU 缓存内容的字符串表示。
// 此方法主要用于调试和日志记录，能够快速查看 LRU 缓存中的所有元素。
// 它通过遍历 LRU 的链表形式存储的元素，并将每个元素的值转换为字符串格式拼接在一起。
// 元素之间以逗号分隔，以便于阅读和分析缓存状态。
func (lru *windowLRU) String() string {
	var s string
	for e := lru.list.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
