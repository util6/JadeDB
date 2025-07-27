/*
JadeDB WAL工厂

本模块实现了WAL组件的工厂模式，提供统一的WAL管理器和恢复管理器创建接口。
支持不同类型的WAL实现，便于扩展和测试。

核心功能：
1. WAL管理器创建：支持文件WAL、内存WAL等不同实现
2. 恢复管理器创建：创建对应的恢复管理器
3. 类型注册：支持动态注册新的WAL实现类型
4. 配置管理：统一的配置选项管理

设计模式：
- 工厂模式：统一创建接口，隐藏实现细节
- 注册模式：支持动态注册新的WAL类型
- 单例模式：全局唯一的工厂实例
*/

package txnwal

import (
	"fmt"
	"strings"
	"sync"
)

// WALType WAL类型枚举
type WALType string

const (
	// FileWAL 文件WAL类型
	FileWAL WALType = "file"
	// MemoryWAL 内存WAL类型（主要用于测试）
	MemoryWAL WALType = "memory"
)

// WALCreator WAL创建器函数类型
type WALCreator func(options *TxnWALOptions) (TxnWALManager, error)

// RecoveryCreator 恢复管理器创建器函数类型
type RecoveryCreator func(walManager TxnWALManager, options *TxnWALOptions, callbacks RecoveryCallbacks) (RecoveryManager, error)

// DefaultWALFactory 默认WAL工厂实现
type DefaultWALFactory struct {
	// 注册的WAL创建器
	walCreators map[WALType]WALCreator
	// 注册的恢复管理器创建器
	recoveryCreators map[WALType]RecoveryCreator
	// 保护并发访问
	mu sync.RWMutex
}

// 全局工厂实例
var (
	globalFactory *DefaultWALFactory
	factoryOnce   sync.Once
)

// GetWALFactory 获取全局WAL工厂实例
func GetWALFactory() WALFactory {
	factoryOnce.Do(func() {
		globalFactory = &DefaultWALFactory{
			walCreators:      make(map[WALType]WALCreator),
			recoveryCreators: make(map[WALType]RecoveryCreator),
		}

		// 注册默认实现
		globalFactory.registerDefaults()
	})

	return globalFactory
}

// CreateTxnWALManager 创建WAL管理器
func (f *DefaultWALFactory) CreateTxnWALManager(options *TxnWALOptions) (TxnWALManager, error) {
	if options == nil {
		options = DefaultTxnWALOptions()
	}

	// 从选项中获取WAL类型，默认为文件WAL
	walType := FileWAL
	if typeStr, ok := options.GetWALType(); ok {
		walType = WALType(strings.ToLower(typeStr))
	}

	f.mu.RLock()
	creator, exists := f.walCreators[walType]
	f.mu.RUnlock()

	if !exists {
		return nil, NewWALError(ErrWALNotFound,
			fmt.Sprintf("unsupported WAL type: %s", walType), nil)
	}

	return creator(options)
}

// CreateRecoveryManager 创建恢复管理器
func (f *DefaultWALFactory) CreateRecoveryManager(walManager TxnWALManager, options *TxnWALOptions) (RecoveryManager, error) {
	// 使用默认回调
	callbacks := &DefaultRecoveryCallbacks{}
	return f.CreateRecoveryManagerWithCallbacks(walManager, options, callbacks)
}

// CreateRecoveryManagerWithCallbacks 创建带回调的恢复管理器
func (f *DefaultWALFactory) CreateRecoveryManagerWithCallbacks(walManager TxnWALManager, options *TxnWALOptions, callbacks RecoveryCallbacks) (RecoveryManager, error) {
	if walManager == nil {
		return nil, NewWALError(ErrWALNotFound, "WAL manager cannot be nil", nil)
	}

	if options == nil {
		options = DefaultTxnWALOptions()
	}

	if callbacks == nil {
		callbacks = &DefaultRecoveryCallbacks{}
	}

	// 从选项中获取WAL类型
	walType := FileWAL
	if typeStr, ok := options.GetWALType(); ok {
		walType = WALType(strings.ToLower(typeStr))
	}

	f.mu.RLock()
	creator, exists := f.recoveryCreators[walType]
	f.mu.RUnlock()

	if !exists {
		return nil, NewWALError(ErrWALNotFound,
			fmt.Sprintf("unsupported recovery manager type: %s", walType), nil)
	}

	return creator(walManager, options, callbacks)
}

// GetSupportedTypes 获取支持的WAL类型
func (f *DefaultWALFactory) GetSupportedTypes() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	types := make([]string, 0, len(f.walCreators))
	for walType := range f.walCreators {
		types = append(types, string(walType))
	}

	return types
}

// RegisterWALCreator 注册WAL创建器
func (f *DefaultWALFactory) RegisterWALCreator(walType WALType, creator WALCreator) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.walCreators[walType] = creator
}

// RegisterRecoveryCreator 注册恢复管理器创建器
func (f *DefaultWALFactory) RegisterRecoveryCreator(walType WALType, creator RecoveryCreator) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.recoveryCreators[walType] = creator
}

// registerDefaults 注册默认实现
func (f *DefaultWALFactory) registerDefaults() {
	// 注册文件WAL创建器
	f.RegisterWALCreator(FileWAL, func(options *TxnWALOptions) (TxnWALManager, error) {
		return NewFileTxnWALManager(options)
	})

	// 注册内存WAL创建器
	f.RegisterWALCreator(MemoryWAL, func(options *TxnWALOptions) (TxnWALManager, error) {
		return NewMemoryTxnWALManager(options)
	})

	// 注册统一恢复管理器创建器
	recoveryCreator := func(walManager TxnWALManager, options *TxnWALOptions, callbacks RecoveryCallbacks) (RecoveryManager, error) {
		return NewUnifiedRecoveryManager(walManager, options, callbacks), nil
	}

	f.RegisterRecoveryCreator(FileWAL, recoveryCreator)
	f.RegisterRecoveryCreator(MemoryWAL, recoveryCreator)
}

// 扩展TxnWALOptions以支持WAL类型配置
type ExtendedTxnWALOptions struct {
	*TxnWALOptions
	WALType string // WAL类型
}

// GetWALType 获取WAL类型
func (opts *TxnWALOptions) GetWALType() (string, bool) {
	// 这里可以从配置中读取WAL类型
	// 简化实现，直接返回默认类型
	return string(FileWAL), true
}

// 便利函数

// CreateFileWAL 创建文件WAL管理器
func CreateFileWAL(options *TxnWALOptions) (TxnWALManager, error) {
	return GetWALFactory().CreateTxnWALManager(options)
}

// CreateMemoryWAL 创建内存WAL管理器
func CreateMemoryWAL(options *TxnWALOptions) (TxnWALManager, error) {
	if options == nil {
		options = DefaultTxnWALOptions()
	}

	// 临时设置WAL类型
	extOptions := &ExtendedTxnWALOptions{
		TxnWALOptions: options,
		WALType:       string(MemoryWAL),
	}

	factory := GetWALFactory()
	return factory.CreateTxnWALManager(extOptions.TxnWALOptions)
}

// CreateRecoveryManagerForWAL 为指定WAL创建恢复管理器
func CreateRecoveryManagerForWAL(walManager TxnWALManager, options *TxnWALOptions, callbacks RecoveryCallbacks) (RecoveryManager, error) {
	factory := GetWALFactory()
	if callbacks != nil {
		return factory.(*DefaultWALFactory).CreateRecoveryManagerWithCallbacks(walManager, options, callbacks)
	}
	return factory.CreateRecoveryManager(walManager, options)
}

// ValidateWALType 验证WAL类型是否支持
func ValidateWALType(walType string) bool {
	factory := GetWALFactory()
	supportedTypes := factory.GetSupportedTypes()

	for _, supportedType := range supportedTypes {
		if strings.EqualFold(supportedType, walType) {
			return true
		}
	}

	return false
}

// GetDefaultWALType 获取默认WAL类型
func GetDefaultWALType() string {
	return string(FileWAL)
}

// TxnWALManagerBuilder WAL管理器构建器
type TxnWALManagerBuilder struct {
	walType   WALType
	options   *TxnWALOptions
	callbacks RecoveryCallbacks
}

// NewTxnWALManagerBuilder 创建WAL管理器构建器
func NewTxnWALManagerBuilder() *TxnWALManagerBuilder {
	return &TxnWALManagerBuilder{
		walType: FileWAL,
		options: DefaultTxnWALOptions(),
	}
}

// WithType 设置WAL类型
func (b *TxnWALManagerBuilder) WithType(walType WALType) *TxnWALManagerBuilder {
	b.walType = walType
	return b
}

// WithOptions 设置WAL选项
func (b *TxnWALManagerBuilder) WithOptions(options *TxnWALOptions) *TxnWALManagerBuilder {
	b.options = options
	return b
}

// WithCallbacks 设置恢复回调
func (b *TxnWALManagerBuilder) WithCallbacks(callbacks RecoveryCallbacks) *TxnWALManagerBuilder {
	b.callbacks = callbacks
	return b
}

// Build 构建WAL管理器和恢复管理器
func (b *TxnWALManagerBuilder) Build() (TxnWALManager, RecoveryManager, error) {
	factory := GetWALFactory()

	// 创建WAL管理器
	walManager, err := factory.CreateTxnWALManager(b.options)
	if err != nil {
		return nil, nil, err
	}

	// 创建恢复管理器
	var recoveryManager RecoveryManager
	if b.callbacks != nil {
		recoveryManager, err = factory.(*DefaultWALFactory).CreateRecoveryManagerWithCallbacks(walManager, b.options, b.callbacks)
	} else {
		recoveryManager, err = factory.CreateRecoveryManager(walManager, b.options)
	}

	if err != nil {
		walManager.Close()
		return nil, nil, err
	}

	return walManager, recoveryManager, nil
}

// BuildWALOnly 只构建WAL管理器
func (b *TxnWALManagerBuilder) BuildWALOnly() (TxnWALManager, error) {
	factory := GetWALFactory()
	return factory.CreateTxnWALManager(b.options)
}
