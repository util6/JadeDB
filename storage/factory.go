/*
JadeDB 存储引擎工厂

本模块实现了统一的存储引擎工厂，负责创建和管理不同类型的存储引擎。
各个引擎包在自己的包中注册创建器。

设计特点：
1. 注册机制：各引擎包自行注册创建器
2. 类型安全：编译时类型检查
3. 配置管理：统一的配置验证
4. 实例管理：引擎实例的生命周期管理
*/

package storage

import (
	"fmt"
	"sync"
)

// DefaultEngineFactory 默认存储引擎工厂
type DefaultEngineFactory struct {
	// 注册的引擎创建器
	mu       sync.RWMutex
	creators map[EngineType]EngineCreator

	// 活跃的引擎实例
	engines map[string]Engine

	// 工厂配置
	config *FactoryConfig
}

// EngineCreator 引擎创建器接口
type EngineCreator interface {
	Create(config interface{}) (Engine, error)
	GetEngineType() EngineType
	GetDefaultConfig() interface{}
	ValidateConfig(config interface{}) error
	GetConfigType() string
}

// FactoryConfig 工厂配置
type FactoryConfig struct {
	// 默认引擎类型
	DefaultEngineType EngineType

	// 是否启用引擎缓存
	EnableEngineCache bool

	// 最大引擎实例数
	MaxEngineInstances int

	// 自动清理配置
	EnableAutoCleanup bool
	CleanupInterval   int // 秒
}

// DefaultFactoryConfig 默认工厂配置
func DefaultFactoryConfig() *FactoryConfig {
	return &FactoryConfig{
		DefaultEngineType:  BPlusTreeEngine,
		EnableEngineCache:  true,
		MaxEngineInstances: 50,
		EnableAutoCleanup:  true,
		CleanupInterval:    300, // 5分钟
	}
}

// 全局工厂实例
var (
	globalFactory *DefaultEngineFactory
	factoryOnce   sync.Once
)

// GetEngineFactory 获取全局存储引擎工厂
func GetEngineFactory() EngineFactory {
	factoryOnce.Do(func() {
		globalFactory = &DefaultEngineFactory{
			creators: make(map[EngineType]EngineCreator),
			engines:  make(map[string]Engine),
			config:   DefaultFactoryConfig(),
		}
	})

	return globalFactory
}

// RegisterEngineCreator 注册引擎创建器
// 这个方法供各引擎包调用来注册自己的创建器
func RegisterEngineCreator(creator EngineCreator) error {
	factory := GetEngineFactory().(*DefaultEngineFactory)

	factory.mu.Lock()
	defer factory.mu.Unlock()

	engineType := creator.GetEngineType()
	if _, exists := factory.creators[engineType]; exists {
		return fmt.Errorf("engine creator for type %s already registered", engineType.String())
	}

	factory.creators[engineType] = creator
	return nil
}

// CreateEngine 创建存储引擎
func (factory *DefaultEngineFactory) CreateEngine(engineType EngineType, config interface{}) (Engine, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	// 检查是否已存在实例（如果启用缓存）
	if factory.config.EnableEngineCache {
		instanceKey := fmt.Sprintf("%s_%p", engineType.String(), config)
		if engine, exists := factory.engines[instanceKey]; exists {
			return engine, nil
		}
	}

	// 检查实例数量限制
	if len(factory.engines) >= factory.config.MaxEngineInstances {
		return nil, fmt.Errorf("maximum engine instances (%d) reached", factory.config.MaxEngineInstances)
	}

	// 获取引擎创建器
	creator, exists := factory.creators[engineType]
	if !exists {
		return nil, fmt.Errorf("unknown engine type: %s", engineType.String())
	}

	// 验证配置
	if config == nil {
		config = creator.GetDefaultConfig()
	} else {
		if err := creator.ValidateConfig(config); err != nil {
			return nil, fmt.Errorf("invalid config for engine %s: %w", engineType.String(), err)
		}
	}

	// 创建引擎
	engine, err := creator.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine %s: %w", engineType.String(), err)
	}

	// 缓存实例
	if factory.config.EnableEngineCache {
		instanceKey := fmt.Sprintf("%s_%p", engineType.String(), config)
		factory.engines[instanceKey] = engine
	}

	return engine, nil
}

// GetSupportedTypes 获取支持的引擎类型
func (factory *DefaultEngineFactory) GetSupportedTypes() []EngineType {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	types := make([]EngineType, 0, len(factory.creators))
	for engineType := range factory.creators {
		types = append(types, engineType)
	}

	return types
}

// GetDefaultConfig 获取默认配置
func (factory *DefaultEngineFactory) GetDefaultConfig(engineType EngineType) interface{} {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	if creator, exists := factory.creators[engineType]; exists {
		return creator.GetDefaultConfig()
	}

	return nil
}

// ValidateConfig 验证配置
func (factory *DefaultEngineFactory) ValidateConfig(engineType EngineType, config interface{}) error {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	creator, exists := factory.creators[engineType]
	if !exists {
		return fmt.Errorf("unknown engine type: %s", engineType.String())
	}

	return creator.ValidateConfig(config)
}

// CreateDefaultEngine 创建默认存储引擎
func (factory *DefaultEngineFactory) CreateDefaultEngine() (Engine, error) {
	return factory.CreateEngine(factory.config.DefaultEngineType, nil)
}

// CloseAllEngines 关闭所有引擎实例
func (factory *DefaultEngineFactory) CloseAllEngines() error {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	var lastErr error
	for key, engine := range factory.engines {
		if err := engine.Close(); err != nil {
			lastErr = err
		}
		delete(factory.engines, key)
	}

	return lastErr
}

// GetEngineInfo 获取引擎信息
func (factory *DefaultEngineFactory) GetEngineInfo(engineType EngineType) (*EngineInfo, error) {
	// 创建临时引擎实例获取信息
	engine, err := factory.CreateEngine(engineType, nil)
	if err != nil {
		return nil, err
	}
	defer engine.Close()

	return engine.GetEngineInfo(), nil
}

// ListActiveEngines 列出活跃的引擎实例
func (factory *DefaultEngineFactory) ListActiveEngines() map[string]EngineType {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	result := make(map[string]EngineType)
	for key, engine := range factory.engines {
		result[key] = engine.GetEngineType()
	}

	return result
}

// GetEngineCount 获取引擎实例数量
func (factory *DefaultEngineFactory) GetEngineCount() int {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	return len(factory.engines)
}

// SetDefaultEngineType 设置默认引擎类型
func (factory *DefaultEngineFactory) SetDefaultEngineType(engineType EngineType) {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	factory.config.DefaultEngineType = engineType
}

// GetDefaultEngineType 获取默认引擎类型
func (factory *DefaultEngineFactory) GetDefaultEngineType() EngineType {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	return factory.config.DefaultEngineType
}
