package cache

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheBasicCRUD(t *testing.T) {
	cache := NewCache(15)
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
		fmt.Printf("set %s: %s\n", key, cache)
	}

	for i := 0; i < 1000; i++ {
		x := rand.Intn(1000)
		key := fmt.Sprintf("key%d", x)

		res, ok := cache.Get(key)
		if x < 10 && x >= 0 && !ok {
			fmt.Printf("第%d次查询未命中get %s: | %s\n", i, key, cache)
		}
		if ok {
			fmt.Printf("第%d次查询命中get %s: | %s\n", i, key, cache)

			//assert.Equal(t, val, res)
			continue
		}
		assert.Equal(t, res, nil)
	}
	fmt.Printf("at last: %s\n", cache)
}
