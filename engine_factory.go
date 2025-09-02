/*
JadeDB 存储引擎工厂

本模块实现了存储引擎的工厂模式，支持动态创建和切换不同类型的存储引擎。
提供了LSM树和B+树引擎的统一管理和无感切换功能。

主要功能：
1. 引擎创建：支持LSM树和B+树引擎的创建
2. 配置管理：为不同引擎提供默认配置
3. 动态切换：支持运行时引擎切换
4. 统一接口：通过storage.Engine接口提供统一访问
*/

package JadeDB

import (
	"fmt"
	"sync"

	"github.com/util6/JadeDB/storage"
)

// EngineType 引擎类型枚举
type EngineType = storage.EngineType

const (
	LSMEngine    = storage.LSMTreeEngine
	BTreeEngine  = storage.BPlusTreeEngine
	HashEngine   = storage.HashTableEngine
	ColumnEngine = storage.ColumnStoreEngine
)

// EngineFactory 存储引擎工厂
type EngineFactory struct {
	mu sync.RWMutex

	// 已创建的引擎实例
	engines map[EngineType]storage.Engine

	// 引擎创建器
	creators map[EngineType]storage.EngineCreator
}

// EngineCreator 引擎创建器接口（使用storage包中的定义）
type EngineCreator = storage.EngineCreator

// NewEngineFactory 创建引擎工厂
func NewEngineFactory() *EngineFactory {
	factory := &EngineFactory{
		engines:  make(map[EngineType]storage.Engine),
		creators: make(map[EngineType]storage.EngineCreator),
	}

	// 从全局注册表加载所有已注册的创建器
	registeredCreators := storage.GetRegisteredCreators()
	for engineType, creator := range registeredCreators {
		factory.creators[engineType] = creator
	}

	return factory
}

// RegisterCreator 注册引擎创建器
func (factory *EngineFactory) RegisterCreator(creator storage.EngineCreator) {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	engineType := creator.GetEngineType()
	factory.creators[engineType] = creator
}

// CreateEngine 创建存储引擎
func (factory *EngineFactory) CreateEngine(engineType EngineType, config interface{}) (storage.Engine, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	// 检查是否已存在该类型的引擎
	if engine, exists := factory.engines[engineType]; exists {
		return engine, nil
	}

	// 查找对应的创建器
	creator, exists := factory.creators[engineType]
	if !exists {
		return nil, fmt.Errorf("unsupported engine type: %v", engineType)
	}

	// 如果没有提供配置，使用默认配置
	if config == nil {
		config = creator.GetDefaultConfig() // 直接调用creator的方法，避免死锁
	}

	// 验证配置
	if err := creator.ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config for engine %v: %w", engineType, err)
	}

	// 创建引擎
	engine, err := creator.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine %v: %w", engineType, err)
	}

	// 缓存引擎实例
	factory.engines[engineType] = engine

	return engine, nil
}

// GetEngine 获取已创建的引擎
func (factory *EngineFactory) GetEngine(engineType EngineType) (storage.Engine, bool) {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	engine, exists := factory.engines[engineType]
	return engine, exists
}

// GetSupportedTypes 获取支持的引擎类型
func (factory *EngineFactory) GetSupportedTypes() []EngineType {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	types := make([]EngineType, 0, len(factory.creators))
	for engineType := range factory.creators {
		types = append(types, engineType)
	}
	return types
}

// GetDefaultConfig 获取默认配置
func (factory *EngineFactory) GetDefaultConfig(engineType EngineType) interface{} {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	creator, exists := factory.creators[engineType]
	if !exists {
		return nil
	}

	return creator.GetDefaultConfig()
}

// ValidateConfig 验证配置
func (factory *EngineFactory) ValidateConfig(engineType EngineType, config interface{}) error {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	creator, exists := factory.creators[engineType]
	if !exists {
		return fmt.Errorf("unsupported engine type: %v", engineType)
	}

	return creator.ValidateConfig(config)
}

// CloseEngine 关闭指定引擎
func (factory *EngineFactory) CloseEngine(engineType EngineType) error {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	engine, exists := factory.engines[engineType]
	if !exists {
		return nil
	}

	if err := engine.Close(); err != nil {
		return fmt.Errorf("failed to close engine %v: %w", engineType, err)
	}

	delete(factory.engines, engineType)
	return nil
}

// CloseAllEngines 关闭所有引擎
func (factory *EngineFactory) CloseAllEngines() error {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	var lastErr error
	for engineType, engine := range factory.engines {
		if err := engine.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close engine %v: %w", engineType, err)
		}
	}

	// 清空引擎映射
	factory.engines = make(map[EngineType]storage.Engine)

	return lastErr
}

// GetEngineInfo 获取引擎信息
func (factory *EngineFactory) GetEngineInfo(engineType EngineType) (*storage.EngineInfo, error) {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	engine, exists := factory.engines[engineType]
	if !exists {
		return nil, fmt.Errorf("engine %v not found", engineType)
	}

	return engine.GetEngineInfo(), nil
}

// GetEngineStats 获取引擎统计信息
func (factory *EngineFactory) GetEngineStats(engineType EngineType) (*storage.EngineStats, error) {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	engine, exists := factory.engines[engineType]
	if !exists {
		return nil, fmt.Errorf("engine %v not found", engineType)
	}

	return engine.GetStats(), nil
}

// ListEngines 列出所有已创建的引擎
func (factory *EngineFactory) ListEngines() map[EngineType]*storage.EngineInfo {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	result := make(map[EngineType]*storage.EngineInfo)
	for engineType, engine := range factory.engines {
		result[engineType] = engine.GetEngineInfo()
	}

	return result
}

// 全局引擎工厂实例
var globalEngineFactory = NewEngineFactory()

// GetGlobalEngineFactory 获取全局引擎工厂
func GetGlobalEngineFactory() *EngineFactory {
	return globalEngineFactory
}
