package bplustree

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSimplePutGet 简单的插入和查找测试
func TestSimplePutGet(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "simple_test")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	options := &BTreeOptions{
		WorkDir:            testDir,
		PageSize:           PageSize,
		BufferPoolSize:     10,
		WALBufferSize:      1024 * 1024,
		CheckpointInterval: time.Minute,
	}

	btree, err := NewBPlusTree(options)
	require.NoError(t, err)
	defer btree.Close()

	// 测试插入
	key := []byte("test_key")
	value := []byte("test_value")

	t.Logf("Root page ID before insert: %d", btree.rootPageID.Load())

	// 先检查根节点的状态
	rootPage, err := btree.bufferPool.GetPage(btree.rootPageID.Load())
	require.NoError(t, err)
	rootNode, err := LoadNode(rootPage)
	require.NoError(t, err)
	t.Logf("Root node type: %v, IsLeaf: %v, Records: %d", rootNode.header.NodeType, rootNode.IsLeaf(), rootNode.GetRecordCount())
	btree.bufferPool.PutPage(rootPage)

	t.Logf("Inserting key: %s, value: %s", key, value)
	err = btree.Put(key, value)
	require.NoError(t, err)
	t.Logf("Root page ID after insert: %d", btree.rootPageID.Load())

	// 测试查找
	t.Logf("Searching for key: %s", key)
	result, err := btree.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, result)

	t.Logf("Found value: %s", result)
}
