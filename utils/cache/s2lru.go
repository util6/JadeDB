/*
JadeDB 分段 LRU 缓存实现

Segmented LRU (S2LRU) 是多层级缓存系统的第二级缓存，实现了两阶段的缓存管理策略。
它将缓存分为 Probation（试用区）和 Protected（保护区）两个区域，提供更智能的缓存管理。

核心设计理念：
1. 两阶段管理：新数据先进入试用区，再次访问后晋升到保护区
2. 热点保护：保护区的数据不易被淘汰，保护真正的热点数据
3. 缓存污染防护：试用区过滤一次性访问的数据，防止污染保护区
4. 自适应调整：根据访问模式动态调整两个区域的数据分布

算法优势：
- 减少缓存污染：一次性访问的数据只在试用区短暂停留
- 保护热点数据：频繁访问的数据在保护区得到长期保护
- 提高命中率：两阶段设计适应不同的访问模式
- 降低抖动：减少热点数据被意外淘汰的情况

工作流程：
1. 新数据进入试用区（Probation）
2. 再次访问时晋升到保护区（Protected）
3. 保护区满时，数据降级到试用区
4. 试用区满时，直接淘汰最久未访问的数据

适用场景：
- 访问模式复杂的应用
- 需要保护热点数据的场景
- 对缓存命中率要求较高的系统
- 存在突发访问模式的工作负载

性能特征：
- 时间复杂度：O(1) 的插入、查找和更新操作
- 空间复杂度：O(n)，n 为缓存容量
- 命中率：通常比传统 LRU 高 10-30%
- 适应性：能够快速适应访问模式的变化
*/

package cache

import (
	"container/list"
	"fmt"
)

// segmentedLRU 实现了两阶段的分段 LRU 缓存算法。
// 它是多层级缓存系统的核心组件，提供智能的热点数据保护机制。
//
// 两阶段设计：
// 1. Stage One (Probation)：试用区，新数据的入口
// 2. Stage Two (Protected)：保护区，经过验证的热点数据
//
// 数据流向：
// 新数据 -> 试用区 -> (再次访问) -> 保护区
//
//	   ↓                        ↓
//	直接淘汰              降级到试用区
//
// 容量分配：
// - 试用区：通常占总容量的 20-40%
// - 保护区：通常占总容量的 60-80%
// - 具体比例可根据工作负载调整
type segmentedLRU struct {
	// 快速查找结构

	// data 是共享的哈希表，提供 O(1) 的键查找性能。
	// 与其他缓存层级共享，避免重复存储映射关系。
	// 键：缓存项的哈希值，值：链表元素指针
	data map[uint64]*list.Element

	// 容量配置

	// stageOneCap 是试用区（Probation）的最大容量。
	// 新数据首先进入这个区域，接受访问频率的考验。
	// 容量设置影响缓存的过滤效果和响应速度。
	stageOneCap int

	// stageTwoCap 是保护区（Protected）的最大容量。
	// 经过验证的热点数据在这里得到长期保护。
	// 容量设置影响热点数据的保护程度。
	stageTwoCap int

	// 数据存储结构

	// stageOne 是试用区的双向链表。
	// 维护试用区数据的访问顺序。
	// 链表头部：最近访问的数据
	// 链表尾部：最久未访问的数据（候选淘汰）
	stageOne *list.List

	// stageTwo 是保护区的双向链表。
	// 维护保护区数据的访问顺序。
	// 链表头部：最近访问的热点数据
	// 链表尾部：相对较冷的数据（候选降级）
	stageTwo *list.List
}

const (
	// STAGE_ONE 标识数据位于试用区（Probation）。
	// 新进入缓存的数据都从这个阶段开始。
	STAGE_ONE = iota + 1

	// STAGE_TWO 标识数据位于保护区（Protected）。
	// 经过验证的热点数据晋升到这个阶段。
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem storeItem) {
	// 先进来的都放 stageOne
	newitem.stage = 1

	// 如果 stageOne 没满，整个 LFU 区域也没满
	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}

	//走到这里说明 StageOne 满了，或者整个 LFU都满了
	//那么需要从 StageOne 淘汰数据了
	e := slru.stageOne.Back()
	item := e.Value.(*storeItem)

	//这里淘汰就是真的淘汰了
	delete(slru.data, item.key)

	*item = newitem

	slru.data[item.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	item := v.Value.(*storeItem)

	// 若访问的缓存数据，已经在 StageTwo，只需要按照 LRU 规则提前即可
	if item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}

	// 若访问的数据还在 StageOne，那么再次被访问到，就需要提升到 StageTwo 阶段了
	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		item.stage = STAGE_TWO
		slru.data[item.key] = slru.stageTwo.PushFront(item)
		return
	}

	// 新数据加入 StageTwo，需要淘汰旧数据
	// StageTwo 中淘汰的数据不会消失，会进入 StageOne
	// StageOne 中，访问频率更低的数据，有可能会被淘汰
	back := slru.stageTwo.Back()
	bitem := back.Value.(*storeItem)

	*bitem, *item = *item, *bitem

	bitem.stage = STAGE_TWO
	item.stage = STAGE_ONE

	slru.data[item.key] = v
	slru.data[bitem.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)
}

func (slru *segmentedLRU) Len() int {
	return slru.stageTwo.Len() + slru.stageOne.Len()
}

func (slru *segmentedLRU) victim() *storeItem {
	//如果 slru 的容量未满，不需要淘汰
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}

	// 如果已经满了，则需要从20%的区域淘汰数据，这里直接从尾部拿最后一个元素即可
	v := slru.stageOne.Back()
	return v.Value.(*storeItem)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*storeItem).value)
	}
	return s
}
