package JadeDB

import (
	"os"
	"testing"
	"time"

	"github.com/util6/JadeDB/utils"
)

var (
	// 测试用的配置选项
	fuzzOpt = &Options{
		WorkDir:          "./fuzz_test",
		SSTableMaxSz:     1 << 10,
		MemTableSize:     1 << 10,
		ValueLogFileSize: 1 << 20,
		ValueThreshold:   0,
		MaxBatchCount:    10,
		MaxBatchSize:     1 << 20,
	}
)

func clearFuzzDir() {
	_, err := os.Stat(fuzzOpt.WorkDir)
	if err == nil {
		os.RemoveAll(fuzzOpt.WorkDir)
	}
	os.Mkdir(fuzzOpt.WorkDir, os.ModePerm)
}

func FuzzAPI(f *testing.F) {
	//添加种子语料，必须和闭包西数的模弼参数一一对应
	f.Add([]byte("core"), []byte("kv"))
	clearFuzzDir()
	db := Open(fuzzOpt)

	defer func() { _ = db.Close() }()
	//运行fuz2引学，不断生成测试用例进行测试
	f.Fuzz(func(t *testing.T, key, value []byte) {

		e := utils.NewEntry(key, value).WithTTL(1 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}

		//道询
		if entry, err := db.Get(key); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s,value=%s,expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
		iter := db.NewIterator(&utils.Options{
			IsAsc: false,
		})
		defer func() { _ = iter.Close() }()
		for iter.Rewind(); iter.Valid(); iter.Next() {
			it := iter.Item()
			t.Logf("db.NewIterator key=%s,value=%s,expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
		}
		t.Logf("db.stats.EntryNum=%+v", db.Info().EntryNum)
		//除
		if err := db.Del(key); err != nil {
			t.Fatal(err)
		}
	})

}
