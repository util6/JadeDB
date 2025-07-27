/*
JadeDB B+树包初始化

本模块负责B+树包的初始化，包括向全局工厂注册B+树引擎创建器。
这样设计可以让各个引擎包自行管理自己的注册逻辑。

设计特点：
1. 自动注册：包导入时自动注册创建器
2. 解耦设计：B+树包不依赖具体的工厂实现
3. 错误处理：注册失败时记录日志
4. 延迟初始化：只在需要时创建引擎实例
*/

package bplustree

import (
	"fmt"
	"log"

	"github.com/util6/JadeDB/storage"
)

// BTreeEngineCreator B+树引擎创建器
type BTreeEngineCreator struct{}

// Create 创建B+树引擎实例
func (creator *BTreeEngineCreator) Create(config interface{}) (storage.Engine, error) {
	var btreeConfig *BTreeEngineConfig

	if config != nil {
		var ok bool
		btreeConfig, ok = config.(*BTreeEngineConfig)
		if !ok {
			return nil, fmt.Errorf("invalid config type for B+Tree engine, expected *BTreeEngineConfig")
		}
	} else {
		btreeConfig = DefaultBTreeEngineConfig()
	}

	return NewBTreeEngine(btreeConfig)
}

// GetEngineType 获取引擎类型
func (creator *BTreeEngineCreator) GetEngineType() storage.EngineType {
	return storage.BPlusTreeEngine
}

// GetDefaultConfig 获取默认配置
func (creator *BTreeEngineCreator) GetDefaultConfig() interface{} {
	return DefaultBTreeEngineConfig()
}

// ValidateConfig 验证配置
func (creator *BTreeEngineCreator) ValidateConfig(config interface{}) error {
	_, ok := config.(*BTreeEngineConfig)
	if !ok {
		return fmt.Errorf("config must be *BTreeEngineConfig")
	}
	return nil
}

// GetConfigType 获取配置类型名称
func (creator *BTreeEngineCreator) GetConfigType() string {
	return "*bplustree.BTreeEngineConfig"
}

// init 包初始化函数，自动注册B+树引擎创建器
func init() {
	creator := &BTreeEngineCreator{}

	if err := storage.RegisterEngineCreator(creator); err != nil {
		log.Printf("Warning: Failed to register B+Tree engine creator: %v", err)
	} else {
		log.Printf("B+Tree engine creator registered successfully")
	}
}
