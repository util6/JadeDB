/*
JadeDB LSM包初始化

本模块负责LSM包的初始化，包括向全局工厂注册LSM引擎创建器。
这样设计可以让各个引擎包自行管理自己的注册逻辑。

设计特点：
1. 自动注册：包导入时自动注册创建器
2. 解耦设计：LSM包不依赖具体的工厂实现
3. 错误处理：注册失败时记录日志
4. 延迟初始化：只在需要时创建引擎实例
*/

package lsm

import (
	"fmt"
	"log"

	"github.com/util6/JadeDB/storage"
)

// LSMEngineCreator LSM引擎创建器
type LSMEngineCreator struct{}

// Create 创建LSM引擎实例
func (creator *LSMEngineCreator) Create(config interface{}) (storage.Engine, error) {
	var lsmConfig *LSMEngineConfig

	if config != nil {
		var ok bool
		lsmConfig, ok = config.(*LSMEngineConfig)
		if !ok {
			return nil, fmt.Errorf("invalid config type for LSM engine, expected *LSMEngineConfig")
		}
	} else {
		lsmConfig = DefaultLSMEngineConfig()
	}

	return NewLSMEngine(lsmConfig)
}

// GetEngineType 获取引擎类型
func (creator *LSMEngineCreator) GetEngineType() storage.EngineType {
	return storage.LSMTreeEngine
}

// GetDefaultConfig 获取默认配置
func (creator *LSMEngineCreator) GetDefaultConfig() interface{} {
	return DefaultLSMEngineConfig()
}

// ValidateConfig 验证配置
func (creator *LSMEngineCreator) ValidateConfig(config interface{}) error {
	_, ok := config.(*LSMEngineConfig)
	if !ok {
		return fmt.Errorf("config must be *LSMEngineConfig")
	}
	return nil
}

// GetConfigType 获取配置类型名称
func (creator *LSMEngineCreator) GetConfigType() string {
	return "*lsm.LSMEngineConfig"
}

// init 包初始化函数，自动注册LSM引擎创建器
func init() {
	creator := &LSMEngineCreator{}

	if err := storage.RegisterEngineCreator(creator); err != nil {
		log.Printf("Warning: Failed to register LSM engine creator: %v", err)
	} else {
		log.Printf("LSM engine creator registered successfully")
	}
}
