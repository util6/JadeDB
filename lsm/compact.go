// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
LSM 树压缩（Compaction）模块

压缩是 LSM 树的核心机制，负责将多个 SSTable 文件合并成更少、更大的文件。
这个过程对于维护 LSM 树的性能和空间效率至关重要。

核心功能：
1. 层级压缩：将上层的文件合并到下层，减少文件数量
2. 数据去重：移除重复和过期的数据，回收存储空间
3. 有序维护：确保每层内部和层间的数据有序性
4. 性能优化：减少读放大，提高查询效率

压缩策略：
- L0 -> L1：特殊处理，因为 L0 文件可能重叠
- L1 -> L2...Ln：标准压缩，选择重叠文件进行合并
- 大小控制：每层有大小限制，超限时触发压缩
- 优先级调度：根据层级大小和文件数量计算压缩优先级

设计原理：
1. 分层压缩：每层大小递增，形成金字塔结构
2. 异步执行：压缩在后台进行，不阻塞读写操作
3. 并发控制：多个压缩器并行工作，提高效率
4. 原子操作：压缩过程保证数据一致性

性能考虑：
- 写放大：压缩会导致数据重复写入，需要平衡
- 读放大：减少文件数量可以降低读放大
- 空间放大：临时存储压缩结果需要额外空间
- CPU 使用：压缩是 CPU 密集型操作

调度算法：
- 优先级计算：基于层级大小和目标大小的比值
- 负载均衡：多个压缩器协调工作，避免冲突
- 自适应调整：根据系统负载动态调整压缩频率
- 资源控制：限制并发压缩数量，避免资源竞争
*/

package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
)

// compactionPriority 表示压缩操作的优先级信息。
// 用于压缩调度器决定哪个层级最需要进行压缩操作。
//
// 优先级计算考虑因素：
// 1. 层级大小与目标大小的比值
// 2. 文件数量与理想数量的比值
// 3. 读写负载和系统资源状况
// 4. 数据的时效性和访问频率
type compactionPriority struct {
	// level 指定需要压缩的层级编号（0-N）。
	// L0 层有特殊的压缩规则，因为文件间可能重叠。
	level int

	// score 是该层级的原始压缩分数。
	// 基于层级大小与目标大小的比值计算。
	// 分数越高，压缩优先级越高。
	score float64

	// adjusted 是调整后的压缩分数。
	// 考虑了系统负载、并发压缩数量等因素。
	// 用于最终的压缩调度决策。
	adjusted float64

	// dropPrefixes 指定在压缩过程中需要丢弃的键前缀。
	// 用于实现数据的逻辑删除和空间回收。
	// 匹配这些前缀的键值对会在压缩时被移除。
	dropPrefixes [][]byte

	// t 包含压缩的目标配置信息。
	// 定义了各层级的目标大小和文件大小限制。
	t targets
}

// targets 定义了 LSM 树各层级的目标配置。
// 这些目标用于指导压缩策略和层级管理。
//
// 配置原理：
// - 每层大小按倍数递增，形成金字塔结构
// - 文件大小在不同层级可以不同，优化访问模式
// - 基础层级决定了整个 LSM 树的容量规划
type targets struct {
	// baseLevel 指定开始应用大小限制的基础层级。
	// 通常是 L1，L0 层有特殊处理规则。
	// 基础层级以下的层级大小会按倍数递增。
	baseLevel int

	// targetSz 定义每个层级的目标总大小（字节）。
	// 数组索引对应层级编号，值为该层级的目标大小。
	// 用于计算压缩优先级和触发压缩操作。
	targetSz []int64

	// fileSz 定义每个层级中单个文件的目标大小（字节）。
	// 不同层级可以有不同的文件大小策略。
	// 影响文件的分割和合并决策。
	fileSz []int64
}

// compactDef 定义了一次具体的压缩操作。
// 包含了执行压缩所需的所有信息和资源。
//
// 压缩流程：
// 1. 选择源层级和目标层级
// 2. 确定参与压缩的文件集合
// 3. 计算键范围和分割策略
// 4. 执行合并和写入操作
// 5. 更新元数据和清理旧文件
type compactDef struct {
	// 压缩器标识和配置

	// compactorId 标识执行此压缩的压缩器编号。
	// 用于日志记录、监控和调试。
	// 多个压缩器可以并行工作。
	compactorId int

	// t 包含压缩的目标配置信息。
	// 从压缩优先级中复制而来。
	t targets

	// p 包含此次压缩的优先级信息。
	// 用于记录压缩的原因和重要性。
	p compactionPriority

	// 层级处理器

	// thisLevel 是源层级的处理器。
	// 提供源文件的访问和管理功能。
	thisLevel *levelHandler

	// nextLevel 是目标层级的处理器。
	// 负责接收压缩后的新文件。
	nextLevel *levelHandler

	// 文件集合

	// top 包含源层级参与压缩的文件。
	// 这些文件的内容会被读取和合并。
	top []*table

	// bot 包含目标层级参与压缩的文件。
	// 这些文件与源文件有键范围重叠。
	bot []*table

	// 键范围管理

	// thisRange 是源层级文件覆盖的键范围。
	// 用于确定需要处理的数据范围。
	thisRange keyRange

	// nextRange 是目标层级文件覆盖的键范围。
	// 包含所有参与压缩的文件的键范围。
	nextRange keyRange

	// splits 定义输出文件的分割点。
	// 用于将压缩结果分割成多个文件。
	splits []keyRange

	// 统计信息

	// thisSize 记录源层级文件的总大小。
	// 用于统计和监控压缩的数据量。
	thisSize int64

	// 数据过滤

	// dropPrefixes 指定需要在压缩时丢弃的键前缀。
	// 实现数据的逻辑删除和空间回收。
	dropPrefixes [][]byte
}

func (cd *compactDef) lockLevels() {
	cd.thisLevel.RLock()
	cd.nextLevel.RLock()
}

func (cd *compactDef) unlockLevels() {
	cd.nextLevel.RUnlock()
	cd.thisLevel.RUnlock()
}

// runCompacter 启动一个compacter
func (lm *levelManager) runCompacter(id int) {
	defer lm.lsm.closer.Done()
	randomDelay := time.NewTimer(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	select {
	case <-randomDelay.C:
	case <-lm.lsm.closer.CloseSignal:
		randomDelay.Stop()
		return
	}
	//TODO 这个值有待验证
	ticker := time.NewTicker(50000 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		// Can add a done channel or other stuff.
		case <-ticker.C:
			lm.runOnce(id)
		case <-lm.lsm.closer.CloseSignal:
			return
		}
	}
}

// runOnce
func (lm *levelManager) runOnce(id int) bool {
	prios := lm.pickCompactLevels()
	if id == 0 {
		// 0号协程 总是倾向于压缩l0层
		prios = moveL0toFront(prios)
	}
	for _, p := range prios {
		if id == 0 && p.level == 0 {
			// 对于l0 无论得分多少都要运行
		} else if p.adjusted < 1.0 {
			// 对于其他level 如果等分小于 则不执行
			break
		}
		if lm.run(id, p) {
			return true
		}
	}
	return false
}

// moveL0toFront 将 level 0 的 compactionPriority 移动到列表的最前面。
// 该函数旨在优先处理 level 0 的压缩任务。
func moveL0toFront(prios []compactionPriority) []compactionPriority {
	// 查找 level 0 的索引位置
	idx := -1
	for i, p := range prios {
		if p.level == 0 {
			idx = i
			break
		}
	}

	// 如果 idx == -1，说明没有找到 L0；如果 idx == 0，说明 L0 已经在最前面，无需处理
	if idx > 0 {
		// 构建新的切片，将 L0 元素移到最前面
		out := append([]compactionPriority{}, prios[idx])
		out = append(out, prios[:idx]...)
		out = append(out, prios[idx+1:]...)
		return out
	}

	// 如果没有找到 L0 或 L0 已经在最前面，直接返回原切片
	return prios
}

// run 执行一个优先级指定的合并任务
func (lm *levelManager) run(id int, p compactionPriority) bool {
	err := lm.doCompact(id, p)
	switch err {
	case nil:
		return true
	case utils.ErrFillTables:
		// 什么也不做，此时合并过程被忽略
	default:
		log.Printf("[taskID:%d] While running doCompact: %v\n ", id, err)
	}
	return false
}

// doCompact 选择level的某些表合并到目标level
func (lm *levelManager) doCompact(id int, p compactionPriority) error {
	l := p.level
	utils.CondPanic(l >= lm.opt.MaxLevelNum, errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum")) // Sanity check.
	if p.t.baseLevel == 0 {
		p.t = lm.levelTargets()
	}
	// 创建真正的压缩计划
	cd := compactDef{
		compactorId:  id,
		p:            p,
		t:            p.t,
		thisLevel:    lm.levels[l],
		dropPrefixes: p.dropPrefixes,
	}

	// 如果是第0层 对齐单独填充处理
	if l == 0 {
		cd.nextLevel = lm.levels[p.t.baseLevel]
		if !lm.fillTablesL0(&cd) {
			return utils.ErrFillTables
		}
	} else {
		cd.nextLevel = cd.thisLevel
		// 如果不是最后一层，则压缩到下一层即可
		if !cd.thisLevel.isLastLevel() {
			cd.nextLevel = lm.levels[l+1]
		}
		if !lm.fillTables(&cd) {
			return utils.ErrFillTables
		}
	}
	// 完成合并后 从合并状态中删除
	defer lm.compactState.delete(cd) // Remove the ranges from compaction status.

	// 执行合并计划
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		log.Printf("[Compactor: %d] LOG Compact FAILED with error: %+v: %+v", id, err, cd)
		return err
	}

	log.Printf("[Compactor: %d] Compaction for level: %d DONE", id, cd.thisLevel.levelNum)
	return nil
}

// pickCompactLevel 选择合适的level执行合并，返回判断的优先级
func (lm *levelManager) pickCompactLevels() (prios []compactionPriority) {
	t := lm.levelTargets()
	addPriority := func(level int, score float64) {
		pri := compactionPriority{
			level:    level,
			score:    score,
			adjusted: score,
			t:        t,
		}
		prios = append(prios, pri)
	}

	// 根据l0表的table数量来对压缩提权
	addPriority(0, float64(lm.levels[0].numTables())/float64(lm.opt.NumLevelZeroTables))

	// 非l0 层都根据大小计算优先级
	for i := 1; i < len(lm.levels); i++ {
		// 处于压缩状态的sst 不能计算在内
		delSize := lm.compactState.delSize(i)
		l := lm.levels[i]
		sz := l.getTotalSize() - delSize
		// score的计算是 扣除正在合并的表后的尺寸与目标sz的比值
		addPriority(i, float64(sz)/float64(t.targetSz[i]))
	}
	utils.CondPanic(len(prios) != len(lm.levels), errors.New("[pickCompactLevels] len(prios) != len(lm.levels)"))

	// 调整得分
	var prevLevel int
	for level := t.baseLevel; level < len(lm.levels); level++ {
		if prios[prevLevel].adjusted >= 1 {
			// 避免过大的得分
			const minScore = 0.01
			if prios[level].score >= minScore {
				prios[prevLevel].adjusted /= prios[level].adjusted
			} else {
				prios[prevLevel].adjusted /= minScore
			}
		}
		prevLevel = level
	}

	// 仅选择得分大于1的压缩内容，并且允许l0到l0的特殊压缩，为了提升查询性能允许l0层独自压缩
	out := prios[:0]
	for _, p := range prios[:len(prios)-1] {
		if p.score >= 1.0 {
			out = append(out, p)
		}
	}
	prios = out

	// 按优先级排序
	sort.Slice(prios, func(i, j int) bool {
		return prios[i].adjusted > prios[j].adjusted
	})
	return prios
}
func (lm *levelManager) lastLevel() *levelHandler {
	return lm.levels[len(lm.levels)-1]
}

// levelTargets
func (lm *levelManager) levelTargets() targets {
	adjust := func(sz int64) int64 {
		if sz < lm.opt.BaseLevelSize {
			return lm.opt.BaseLevelSize
		}
		return sz
	}

	// 初始化默认都是最大层级
	t := targets{
		targetSz: make([]int64, len(lm.levels)),
		fileSz:   make([]int64, len(lm.levels)),
	}
	// 从最后一个level开始计算
	dbSize := lm.lastLevel().getTotalSize()
	for i := len(lm.levels) - 1; i > 0; i-- {
		leveTargetSize := adjust(dbSize)
		t.targetSz[i] = leveTargetSize
		// 如果当前的level没有达到合并的要求
		if t.baseLevel == 0 && leveTargetSize <= lm.opt.BaseLevelSize {
			t.baseLevel = i
		}
		dbSize /= int64(lm.opt.LevelSizeMultiplier)
	}

	tsz := lm.opt.BaseTableSize
	for i := 0; i < len(lm.levels); i++ {
		if i == 0 {
			// l0选择memtable的size作为文件的尺寸
			t.fileSz[i] = lm.opt.MemTableSize
		} else if i <= t.baseLevel {
			t.fileSz[i] = tsz
		} else {
			tsz *= int64(lm.opt.TableSizeMultiplier)
			t.fileSz[i] = tsz
		}
	}

	// 找到最后一个空level作为目标level实现跨level归并，减少写放大
	for i := t.baseLevel + 1; i < len(lm.levels)-1; i++ {
		if lm.levels[i].getTotalSize() > 0 {
			break
		}
		t.baseLevel = i
	}

	// 如果存在断层，则目标level++
	b := t.baseLevel
	lvl := lm.levels
	if b < len(lvl)-1 && lvl[b].getTotalSize() == 0 && lvl[b+1].getTotalSize() < t.targetSz[b+1] {
		t.baseLevel++
	}
	return t
}

type thisAndNextLevelRLocked struct{}

// fillTables 选择给定层级中需要压缩的表格。
// 参数：
//
//	cd - 压缩操作的定义，包含当前层级和下一层级的信息。
//
// 返回值：
//
//	如果成功选择了需要压缩的表格，则返回 true；否则返回 false。
func (lm *levelManager) fillTables(cd *compactDef) bool {
	// 锁定层级以确保线程安全
	cd.lockLevels()
	defer cd.unlockLevels()

	// 创建一个新的表格切片并复制当前层级的所有表格
	tables := make([]*table, cd.thisLevel.numTables())
	copy(tables, cd.thisLevel.tables)
	if len(tables) == 0 {
		return false
	}

	// 如果是最后一层，根据陈旧数据大小选择表格进行压缩
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}

	// 根据启发式规则对表格进行排序，优先压缩较旧的表格
	lm.sortByHeuristic(tables, cd)

	// 遍历所有表格，尝试选择合适的表格进行压缩
	for _, t := range tables {
		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)

		// 如果该范围已经被压缩过，则跳过此表格
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// 设置当前层级的顶部表格，并查找下一层级中与此范围重叠的表格
		cd.top = []*table{t}
		left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)

		// 复制下一层级中与此范围重叠的表格到 cd.bot
		cd.bot = make([]*table, right-left)
		copy(cd.bot, cd.nextLevel.tables[left:right])

		// 如果下一层级没有与此范围重叠的表格，则直接添加压缩任务并返回
		if len(cd.bot) == 0 {
			cd.bot = []*table{}
			cd.nextRange = cd.thisRange
			if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
				continue
			}
			return true
		}

		// 计算下一层级中与此范围重叠的表格的关键范围
		cd.nextRange = getKeyRange(cd.bot...)

		// 如果下一层级的关键范围已经被压缩过，则跳过此表格
		if lm.compactState.overlapsWith(cd.nextLevel.levelNum, cd.nextRange) {
			continue
		}

		// 尝试添加压缩任务，如果成功则返回
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			continue
		}
		return true
	}

	// 如果没有找到合适的表格进行压缩，则返回 false
	return false
}

// compact older tables first.
func (lm *levelManager) sortByHeuristic(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}

	// Sort tables by max version. This is what RocksDB does.
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].ss.Indexs().MaxVersion < tables[j].ss.Indexs().MaxVersion
	})
}

// runCompactDef 执行特定级别的数据压缩操作。
// 参数:
// - id: 压缩操作的标识符。
// - l: 当前操作的级别。
// - cd: 压缩定义，包含了压缩操作所需的信息。
// 返回值:
// - error: 如果操作失败，返回错误信息。
func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
	// 检查文件大小列表是否为空，如果为空则返回错误。
	if len(cd.t.fileSz) == 0 {
		return errors.New("Filesizes cannot be zero. Targets are not set")
	}
	// 记录操作开始时间。
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	// 确保分割键列表不为空。
	utils.CondPanic(len(cd.splits) != 0, errors.New("len(cd.splits) != 0"))
	// 根据当前级别和下一级别是否相同来决定是否添加分割键。
	if thisLevel == nextLevel {
		// l0 to l0 和 lmax to lmax 不做特殊处理
	} else {
		lm.addSplits(&cd)
	}
	// 追加一个空的分割键范围，如果当前分割键列表为空。
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, keyRange{})
	}

	// 构建新的 SSTable。
	newTables, decr, err := lm.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	// 确保在函数退出时执行decr，除非已经有错误发生。
	defer func() {
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	// 构建变更集。
	changeSet := buildChangeSet(&cd, newTables)

	// 更新manifest文件。
	if err := lm.manifestFile.AddChanges(changeSet.Changes); err != nil {
		return err
	}

	// 用新的SSTable替换下一级别的SSTable。
	if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
		return err
	}
	// 在函数退出时减少引用计数。
	defer decrRefs(cd.top)
	// 删除当前级别的SSTable。
	if err := thisLevel.deleteTables(cd.top); err != nil {
		return err
	}

	// 记录日志，如果操作耗时较长。
	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		var expensive string
		if dur > time.Second {
			expensive = " [E]"
		}
		fmt.Printf("[%d]%s LOG Compact %d->%d (%d, %d -> %d tables with %d splits)."+
			" [%s] -> [%s], took %v\n",
			id, expensive, thisLevel.levelNum, nextLevel.levelNum, len(cd.top), len(cd.bot),
			len(newTables), len(cd.splits), strings.Join(from, " "), strings.Join(to, " "),
			dur.Round(time.Millisecond))
	}
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) pb.ManifestChangeSet {
	changes := []*pb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
	}
	return pb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id: id,
		Op: pb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *pb.ManifestChange {
	return &pb.ManifestChange{
		Id:    id,
		Op:    pb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables 合并两个层的sst文件
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	topTables := cd.top
	botTables := cd.bot
	iterOpt := &utils.Options{
		IsAsc: true,
	}
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []utils.Iterator {
		// Create iterators across all the tables involved first.
		var iters []utils.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = []utils.Iterator{topTables[0].NewIterator(iterOpt)}
		}
		return append(iters, NewConcatIterator(botTables, iterOpt))
	}

	// 开始并行执行压缩过程
	res := make(chan *table, 3)
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		// Initiate Do here so we can register the goroutines for buildTables too.
		done := inflightBuilders.Do()
		// 开启一个协程去处理子压缩
		go func(kr keyRange) {
			defer done()
			defer inflightBuilders.Done()
			it := NewMergeIterator(newIterator(), false)
			defer it.Close()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
		}(kr)
	}

	// mapreduce的方式收集table的句柄
	var newTables []*table
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for t := range res {
			newTables = append(newTables, t)
		}
	}()

	// 在这里等待所有的压缩过程完成
	inflightBuilders.Finish()
	var err error
	// channel 资源回收
	close(res)
	// 等待所有的builder刷到磁盘
	wg.Wait()

	if err == nil {
		// 同步刷盘，保证数据一定落盘
		err = utils.SyncDir(lm.opt.WorkDir)
	}

	if err != nil {
		// 如果出现错误，则删除索引新创建的文件
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return utils.CompareKeys(newTables[i].ss.MaxKey(), newTables[j].ss.MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

// 并行的运行子压缩情况
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	// Let's say we have 10 tables in cd.bot and min width = 3. Then, we'll pick
	// 0, 1, 2 (pick), 3, 4, 5 (pick), 6, 7, 8 (pick), 9 (pick, because last table).
	// This gives us 4 picks for 10 tables.
	// In an edge case, 142 tables in bottom led to 48 splits. That's too many splits, because it
	// then uses up a lot of memory for table builder.
	// We should keep it so we have at max 5 splits.
	width := int(math.Ceil(float64(len(cd.bot)) / 5.0))
	if width < 3 {
		width = 3
	}
	skr := cd.thisRange
	skr.extend(cd.nextRange)

	addRange := func(right []byte) {
		skr.right = utils.Copy(right)
		cd.splits = append(cd.splits, skr)
		skr.left = skr.right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// 设置最大值为右区间
			right := utils.KeyWithTs(utils.ParseKey(t.ss.MaxKey()), math.MaxUint64)
			addRange(right)
		}
	}
}

// sortByStaleData 对表中陈旧数据的数量对sst文件进行排序
func (lm *levelManager) sortByStaleDataSize(tables []*table, cd *compactDef) {
	if len(tables) == 0 || cd.nextLevel == nil {
		return
	}
	// TODO 统计一个 sst文件中陈旧数据的数量，涉及对存储格式的修改
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].StaleDataSize() > tables[j].StaleDataSize()
	})
}

// max level 和 max level 的压缩
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	sortedTables := make([]*table, len(tables))
	copy(sortedTables, tables)
	lm.sortByStaleDataSize(sortedTables, cd)

	if len(sortedTables) > 0 && sortedTables[0].StaleDataSize() == 0 {
		// This is a maxLevel to maxLevel compaction and we don't have any stale data.
		return false
	}
	cd.bot = []*table{}
	collectBotTables := func(t *table, needSz int64) {
		totalSize := t.Size()

		j := sort.Search(len(tables), func(i int) bool {
			return utils.CompareKeys(tables[i].ss.MinKey(), t.ss.MinKey()) >= 0
		})
		utils.CondPanic(tables[j].fid != t.fid, errors.New("tables[j].ID() != t.ID()"))
		j++
		// Collect tables until we reach the the required size.
		for j < len(tables) {
			newT := tables[j]
			totalSize += newT.Size()

			if totalSize >= needSz {
				break
			}
			cd.bot = append(cd.bot, newT)
			cd.nextRange.extend(getKeyRange(newT))
			j++
		}
	}
	now := time.Now()
	for _, t := range sortedTables {
		if now.Sub(*t.GetCreatedAt()) < time.Hour {
			// Just created it an hour ago. Don't pick for compaction.
			continue
		}
		// If the stale data size is less than 10 MB, it might not be worth
		// rewriting the table. Skip it.
		if t.StaleDataSize() < 10<<20 {
			continue
		}

		cd.thisSize = t.Size()
		cd.thisRange = getKeyRange(t)
		// Set the next range as the same as the current range. If we don't do
		// this, we won't be able to run more than one max level compactions.
		cd.nextRange = cd.thisRange
		// If we're already compacting this range, don't do anything.
		if lm.compactState.overlapsWith(cd.thisLevel.levelNum, cd.thisRange) {
			continue
		}

		// Found a valid table!
		cd.top = []*table{t}

		needFileSz := cd.t.fileSz[cd.thisLevel.levelNum]
		// 如果合并的sst size需要的文件尺寸直接终止
		if t.Size() >= needFileSz {
			break
		}
		// TableSize is less than what we want. Collect more tables for compaction.
		// If the level has multiple small tables, we collect all of them
		// together to form a bigger table.
		collectBotTables(t, needFileSz)
		if !lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd) {
			cd.bot = cd.bot[:0]
			cd.nextRange = keyRange{}
			continue
		}
		return true
	}
	if len(cd.top) == 0 {
		return false
	}

	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0 先尝试从l0 到lbase的压缩，如果失败则对l0自己压缩
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		utils.Panic(errors.New("base level can be zero"))
	}
	// 如果优先级低于1 则不执行
	if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}

	var out []*table
	var kr keyRange
	// cd.top[0] 是最老的文件，从最老的文件开始
	for _, t := range top {
		dkr := getKeyRange(t)
		if kr.overlapsWith(dkr) {
			out = append(out, t)
			kr.extend(dkr)
		} else {
			// 如果有任何一个不重合的区间存在则直接终止
			break
		}
	}
	// 获取目标range list 的全局 range 对象
	cd.thisRange = getKeyRange(out...)
	cd.top = out

	left, right := cd.nextLevel.overlappingTables(levelHandlerRLocked{}, cd.thisRange)
	cd.bot = make([]*table, right-left)
	copy(cd.bot, cd.nextLevel.tables[left:right])

	if len(cd.bot) == 0 {
		cd.nextRange = cd.thisRange
	} else {
		cd.nextRange = getKeyRange(cd.bot...)
	}
	return lm.compactState.compareAndAdd(thisAndNextLevelRLocked{}, *cd)
}

// fillTablesL0ToL0 l0到l0压缩
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	if cd.compactorId != 0 {
		// 只要0号压缩处理器可以执行，避免l0tol0的资源竞争
		return false
	}

	cd.nextLevel = lm.levels[0]
	cd.nextRange = keyRange{}
	cd.bot = nil

	//  TODO 这里是否会导致死锁？
	utils.CondPanic(cd.thisLevel.levelNum != 0, errors.New("cd.thisLevel.levelNum != 0"))
	utils.CondPanic(cd.nextLevel.levelNum != 0, errors.New("cd.nextLevel.levelNum != 0"))
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	lm.compactState.Lock()
	defer lm.compactState.Unlock()

	top := cd.thisLevel.tables
	var out []*table
	now := time.Now()
	for _, t := range top {
		if t.Size() >= 2*cd.t.fileSz[0] {
			// 在L0 to L0 的压缩过程中，不要对过大的sst文件压缩，这会造成性能抖动
			continue
		}
		if now.Sub(*t.GetCreatedAt()) < 10*time.Second {
			// 如果sst的创建时间不足10s 也不要回收
			continue
		}
		// 如果当前的sst 已经在压缩状态 也应该忽略
		if _, beingCompacted := lm.compactState.tables[t.fid]; beingCompacted {
			continue
		}
		out = append(out, t)
	}

	if len(out) < 4 {
		// 满足条件的sst小于4个那就不压缩了
		return false
	}
	cd.thisRange = infRange
	cd.top = out

	// 在这个过程中避免任何l0到其他层的合并
	thisLevel := lm.compactState.levels[cd.thisLevel.levelNum]
	thisLevel.ranges = append(thisLevel.ranges, infRange)
	for _, t := range out {
		lm.compactState.tables[t.fid] = struct{}{}
	}

	//  l0 to l0的压缩最终都会压缩为一个文件，这大大减少了l0层文件数量，减少了读放大
	cd.t.fileSz[0] = math.MaxUint32
	return true
}

// getKeyRange 返回一组sst的区间合并后的最大与最小值
func getKeyRange(tables ...*table) keyRange {
	if len(tables) == 0 {
		return keyRange{}
	}
	minKey := tables[0].ss.MinKey()
	maxKey := tables[0].ss.MaxKey()
	for i := 1; i < len(tables); i++ {
		if utils.CompareKeys(tables[i].ss.MinKey(), minKey) < 0 {
			minKey = tables[i].ss.MinKey()
		}
		if utils.CompareKeys(tables[i].ss.MaxKey(), maxKey) > 0 {
			maxKey = tables[i].ss.MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	return keyRange{
		left:  utils.KeyWithTs(utils.ParseKey(minKey), math.MaxUint64),
		right: utils.KeyWithTs(utils.ParseKey(maxKey), 0),
	}
}

func iteratorsReversed(th []*table, opt *utils.Options) []utils.Iterator {
	out := make([]utils.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}
func (lm *levelManager) updateDiscardStats(discardStats map[uint32]int64) {
	select {
	case *lm.lsm.option.DiscardStatsCh <- discardStats:
	default:
	}
}

// 真正执行并行压缩的子压缩文件
func (lm *levelManager) subcompact(it utils.Iterator, kr keyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte
	// 更新 discardStats
	discardStats := make(map[uint32]int64)
	defer func() {
		lm.updateDiscardStats(discardStats)
	}()
	updateStats := func(e *utils.Entry) {
		if e.Meta&utils.BitValuePointer > 0 {
			var vp utils.ValuePtr
			vp.Decode(e.Value)
			discardStats[vp.Fid] += int64(vp.Len)
		}
	}
	addKeys := func(builder *tableBuilder) {
		var tableKr keyRange
		for ; it.Valid(); it.Next() {
			key := it.Item().Entry().Key
			//version := utils.ParseTs(key)
			isExpired := isDeletedOrExpired(0, it.Item().Entry().ExpiresAt)
			if !utils.SameKey(key, lastKey) {
				// 如果迭代器返回的key大于当前key的范围就不用执行了
				if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					// 如果超过预估的sst文件大小，则直接结束
					break
				}
				// 把当前的key变为 lastKey
				lastKey = utils.SafeCopy(lastKey, key)
				//umVersions = 0
				// 如果左边界没有，则当前key给到左边界
				if len(tableKr.left) == 0 {
					tableKr.left = utils.SafeCopy(tableKr.left, key)
				}
				// 更新右边界
				tableKr.right = lastKey
			}
			// TODO 这里要区分值的指针
			// 判断是否是过期内容，是的话就删除
			switch {
			case isExpired:
				updateStats(it.Item().Entry())
				builder.AddStaleKey(it.Item().Entry())
			default:
				builder.AddKey(it.Item().Entry())
			}
		}
	} // End of function: addKeys

	//如果 key range left还存在 则seek到这里 说明遍历中途停止了
	if len(kr.left) > 0 {
		it.Seek(kr.left)
	} else {
		//
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.right) > 0 && utils.CompareKeys(key, kr.right) >= 0 {
			break
		}
		// 拼装table创建的参数
		// TODO 这里可能要大改，对open table的参数复制一份opt
		builder := newTableBuilderWithSSTSize(lm.opt, cd.t.fileSz[cd.nextLevel.levelNum])

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			builder.finish()
			builder.Close()
			continue
		}
		if err := inflightBuilders.Do(); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
		// 充分发挥 ssd的并行 写入特性
		go func(builder *tableBuilder) {
			defer inflightBuilders.Done()
			defer builder.Close()
			var tbl *table
			newFID := atomic.AddUint64(&lm.maxFID, 1) // compact的时候是没有memtable的，这里自增maxFID即可。
			// TODO 这里的sst文件需要根据level大小变化
			sstName := utils.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl = openTable(lm, sstName, builder)
			if tbl == nil {
				return
			}
			res <- tbl
		}(builder)
	}
}

// checkOverlap 检查是否与下一层存在重合
func (lm *levelManager) checkOverlap(tables []*table, lev int) bool {
	kr := getKeyRange(tables...)
	for i, lh := range lm.levels {
		if i < lev { // Skip upper levels.
			continue
		}
		lh.RLock()
		left, right := lh.overlappingTables(levelHandlerRLocked{}, kr)
		lh.RUnlock()
		if right-left > 0 {
			return true
		}
	}
	return false
}

// 判断是否过期 是可删除
func isDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

// compactStatus
type compactStatus struct {
	sync.RWMutex
	levels []*levelCompactStatus
	tables map[uint64]struct{}
}

// newCompactStatus 创建并初始化一个新的压缩状态对象。
// 返回值是一个指向 compactStatus 结构的指针，它包含了 LSM 树的压缩状态。
func (lsm *LSM) newCompactStatus() *compactStatus {
	// 初始化压缩状态对象 cs。
	cs := &compactStatus{
		levels: make([]*levelCompactStatus, 0),
		tables: make(map[uint64]struct{}),
	}

	// 遍历 LSM 树的每一个级别，初始化每个级别的压缩状态。
	for i := 0; i < lsm.option.MaxLevelNum; i++ {
		// 为每个级别添加一个新的 levelCompactStatus 对象。
		cs.levels = append(cs.levels, &levelCompactStatus{})
	}

	// 返回初始化完毕的压缩状态对象。
	return cs
}

func (cs *compactStatus) overlapsWith(level int, this keyRange) bool {
	cs.RLock()
	defer cs.RUnlock()

	thisLevel := cs.levels[level]
	return thisLevel.overlapsWith(this)
}

func (cs *compactStatus) delSize(l int) int64 {
	cs.RLock()
	defer cs.RUnlock()
	return cs.levels[l].delSize
}

func (cs *compactStatus) delete(cd compactDef) {
	cs.Lock()
	defer cs.Unlock()

	tl := cd.thisLevel.levelNum

	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	thisLevel.delSize -= cd.thisSize
	found := thisLevel.remove(cd.thisRange)
	// The following check makes sense only if we're compacting more than one
	// table. In case of the max level, we might rewrite a single table to
	// remove stale data.
	if cd.thisLevel != cd.nextLevel && !cd.nextRange.isEmpty() {
		found = nextLevel.remove(cd.nextRange) && found
	}

	if !found {
		this := cd.thisRange
		next := cd.nextRange
		fmt.Printf("Looking for: %s in this level %d.\n", this, tl)
		fmt.Printf("This Level:\n%s\n", thisLevel.debug())
		fmt.Println()
		fmt.Printf("Looking for: %s in next level %d.\n", next, cd.nextLevel.levelNum)
		fmt.Printf("Next Level:\n%s\n", nextLevel.debug())
		log.Fatal("keyRange not found")
	}
	for _, t := range append(cd.top, cd.bot...) {
		_, ok := cs.tables[t.fid]
		utils.CondPanic(!ok, fmt.Errorf("cs.tables is nil"))
		delete(cs.tables, t.fid)
	}
}

// compareAndAdd 检查当前压缩状态是否满足特定条件，如果满足，则将压缩定义 (cd) 添加到压缩状态 (cs) 中。
// 该函数在当前层和下一层范围已经锁定的情况下被调用。

func (cs *compactStatus) compareAndAdd(_ thisAndNextLevelRLocked, cd compactDef) bool {
	// 加锁确保线程安全
	cs.Lock()
	defer cs.Unlock()

	// 获取当前层编号并检查是否超出最大层数限制
	tl := cd.thisLevel.levelNum
	utils.CondPanic(tl >= len(cs.levels), fmt.Errorf("Got level %d. Max levels: %d", tl, len(cs.levels)))

	// 获取当前层和下一层的信息
	thisLevel := cs.levels[cd.thisLevel.levelNum]
	nextLevel := cs.levels[cd.nextLevel.levelNum]

	// 检查当前层和下一层是否有重叠范围，如果有则返回 false
	if thisLevel.overlapsWith(cd.thisRange) {
		return false
	}
	if nextLevel.overlapsWith(cd.nextRange) {
		return false
	}

	// 检查当前层是否确实需要进行压缩。注意：这里不应再检查大小，因为压缩优先级已经做了大小检查。
	// 这里只是执行其他组件的压缩意愿。

	// 更新当前层和下一层的范围信息，并增加删除大小
	thisLevel.ranges = append(thisLevel.ranges, cd.thisRange)
	nextLevel.ranges = append(nextLevel.ranges, cd.nextRange)
	thisLevel.delSize += cd.thisSize

	// 将涉及的表信息添加到压缩状态中
	for _, t := range append(cd.top, cd.bot...) {
		cs.tables[t.fid] = struct{}{}
	}

	// 返回 true 表示成功添加压缩定义
	return true
}

// levelCompactStatus
type levelCompactStatus struct {
	ranges  []keyRange
	delSize int64
}

func (lcs *levelCompactStatus) overlapsWith(dst keyRange) bool {
	for _, r := range lcs.ranges {
		if r.overlapsWith(dst) {
			return true
		}
	}
	return false
}
func (lcs *levelCompactStatus) remove(dst keyRange) bool {
	final := lcs.ranges[:0]
	var found bool
	for _, r := range lcs.ranges {
		if !r.equals(dst) {
			final = append(final, r)
		} else {
			found = true
		}
	}
	lcs.ranges = final
	return found
}

func (lcs *levelCompactStatus) debug() string {
	var b bytes.Buffer
	for _, r := range lcs.ranges {
		b.WriteString(r.String())
	}
	return b.String()
}

// keyRange
type keyRange struct {
	left  []byte
	right []byte
	inf   bool
	size  int64 // size is used for Key splits.
}

func (r keyRange) isEmpty() bool {
	return len(r.left) == 0 && len(r.right) == 0 && !r.inf
}

var infRange = keyRange{inf: true}

func (r keyRange) String() string {
	return fmt.Sprintf("[left=%x, right=%x, inf=%v]", r.left, r.right, r.inf)
}

func (r keyRange) equals(dst keyRange) bool {
	return bytes.Equal(r.left, dst.left) &&
		bytes.Equal(r.right, dst.right) &&
		r.inf == dst.inf
}

func (r *keyRange) extend(kr keyRange) {
	if kr.isEmpty() {
		return
	}
	if r.isEmpty() {
		*r = kr
	}
	if len(r.left) == 0 || utils.CompareKeys(kr.left, r.left) < 0 {
		r.left = kr.left
	}
	if len(r.right) == 0 || utils.CompareKeys(kr.right, r.right) > 0 {
		r.right = kr.right
	}
	if kr.inf {
		r.inf = true
	}
}

func (r keyRange) overlapsWith(dst keyRange) bool {
	// Empty keyRange always overlaps.
	if r.isEmpty() {
		return true
	}
	// Empty dst doesn't overlap with anything.
	if dst.isEmpty() {
		return false
	}
	if r.inf || dst.inf {
		return true
	}

	// [dst.left, dst.right] ... [r.left, r.right]
	// If my left is greater than dst right, we have no overlap.
	if utils.CompareKeys(r.left, dst.right) > 0 {
		return false
	}
	// [r.left, r.right] ... [dst.left, dst.right]
	// If my right is less than dst left, we have no overlap.
	if utils.CompareKeys(r.right, dst.left) < 0 {
		return false
	}
	// We have overlap.
	return true
}
