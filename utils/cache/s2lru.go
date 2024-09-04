package cache

import (
	"container/list"
	"fmt"
)

// segmentedLRU 是一个分段的最近最少使用 (LRU) 缓存结构。
// 它将缓存分为两个阶段管理，每个阶段有不同的容量限制，
// 通过这种分段策略来优化缓存的使用效率和性能。
type segmentedLRU struct {
	// data 是一个映射表，用于快速查找缓存中的数据。
	// 键是缓存项的唯一标识，值是该缓存项在列表中的元素指针。
	data map[uint64]*list.Element

	// stageOneCap 和 stageTwoCap 分别代表两个阶段的容量限制。
	// stageOneCap 是第一阶段缓存的容量上限。
	stageOneCap int
	// stageTwoCap 是第二阶段缓存的容量上限。
	stageTwoCap int

	// stageOne 和 stageTwo 分别是两个阶段的缓存列表。
	// 使用 list.List 来维护缓存项的访问顺序，最近访问的项位于列表末端。
	stageOne *list.List
	stageTwo *list.List
}

const (
	STAGE_ONE = iota + 1
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
