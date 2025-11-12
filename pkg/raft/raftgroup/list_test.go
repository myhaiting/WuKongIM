package raftgroup

import (
	"fmt"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// mockRaft 用于测试的 mock IRaft 实现
type mockRaft struct {
	key string
	mu  sync.Mutex
}

func (m *mockRaft) Key() string              { return m.key }
func (m *mockRaft) Step(e types.Event) error { return nil }
func (m *mockRaft) Ready() []types.Event     { return nil }
func (m *mockRaft) HasReady() bool           { return false }
func (m *mockRaft) Tick()                    {}
func (m *mockRaft) IsLeader() bool           { return false }
func (m *mockRaft) LeaderId() uint64         { return 0 }
func (m *mockRaft) Config() types.Config     { return types.Config{} }
func (m *mockRaft) AppliedIndex() uint64     { return 0 }
func (m *mockRaft) CommittedIndex() uint64   { return 0 }
func (m *mockRaft) KeepAlive()               {}
func (m *mockRaft) LastLogIndex() uint64     { return 0 }
func (m *mockRaft) LastTerm() uint32         { return 0 }
func (m *mockRaft) NodeId() uint64           { return 0 }
func (m *mockRaft) Lock()                    { m.mu.Lock() }
func (m *mockRaft) Unlock()                  { m.mu.Unlock() }

// 基准测试：push 操作
func BenchmarkLinkedList_Push(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				ll := newLinkedList()
				b.StartTimer()

				for j := 0; j < size; j++ {
					ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", j)})
				}
			}
		})
	}
}

// 基准测试：get 操作（这是主要的性能瓶颈）
func BenchmarkLinkedList_Get(b *testing.B) {
	sizes := []int{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			// 预先填充数据
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 查找中间位置的元素（平均情况）
				key := fmt.Sprintf("raft_%d", size/2)
				ll.get(key)
			}
		})
	}
}

// 基准测试：get 操作（最坏情况 - 查找最后一个元素）
func BenchmarkLinkedList_GetWorstCase(b *testing.B) {
	sizes := []int{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 查找最后一个元素
				key := fmt.Sprintf("raft_%d", size-1)
				ll.get(key)
			}
		})
	}
}

// 基准测试：readHandlers 操作（另一个性能瓶颈）
func BenchmarkLinkedList_ReadHandlers(b *testing.B) {
	sizes := []int{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rafts := make([]IRaft, 0, size)
				ll.readHandlers(&rafts)
			}
		})
	}
}

// 基准测试：readHandlers 操作（模拟真实场景 - 有变更）
func BenchmarkLinkedList_ReadHandlers_WithChanges(b *testing.B) {
	sizes := []int{10, 50, 100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 每10次读取，进行一次修改（模拟真实场景）
				if i%10 == 0 && i > 0 {
					ll.push(&mockRaft{key: fmt.Sprintf("raft_new_%d", i)})
					ll.remove(fmt.Sprintf("raft_new_%d", i))
				}
				rafts := make([]IRaft, 0, size)
				ll.readHandlers(&rafts)
			}
		})
	}
}

// 基准测试：混合操作（模拟真实使用场景）
func BenchmarkLinkedList_Mixed(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 模拟真实场景：90% get, 8% readHandlers, 2% push/remove
				op := i % 100
				if op < 90 {
					// get 操作
					key := fmt.Sprintf("raft_%d", i%size)
					ll.get(key)
				} else if op < 98 {
					// readHandlers 操作
					rafts := make([]IRaft, 0, size)
					ll.readHandlers(&rafts)
				} else {
					// push/remove 操作
					newKey := fmt.Sprintf("raft_temp_%d", i)
					ll.push(&mockRaft{key: newKey})
					ll.remove(newKey)
				}
			}
		})
	}
}

// 基准测试：并发 get 操作
func BenchmarkLinkedList_ConcurrentGet(b *testing.B) {
	sizes := []int{10, 50, 100, 500}
	goroutines := []int{1, 2, 4, 8, 16}

	for _, size := range sizes {
		for _, numGoroutines := range goroutines {
			b.Run(fmt.Sprintf("size_%d_goroutines_%d", size, numGoroutines), func(b *testing.B) {
				ll := newLinkedList()
				for i := 0; i < size; i++ {
					ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
				}

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						key := fmt.Sprintf("raft_%d", i%size)
						ll.get(key)
						i++
					}
				})
			})
		}
	}
}

// 基准测试：并发 readHandlers 操作
func BenchmarkLinkedList_ConcurrentReadHandlers(b *testing.B) {
	sizes := []int{10, 50, 100, 500}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					rafts := make([]IRaft, 0, size)
					ll.readHandlers(&rafts)
				}
			})
		})
	}
}

// 基准测试：并发混合操作（读多写少）
func BenchmarkLinkedList_ConcurrentMixed(b *testing.B) {
	sizes := []int{50, 100, 500}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ll := newLinkedList()
			for i := 0; i < size; i++ {
				ll.push(&mockRaft{key: fmt.Sprintf("raft_%d", i)})
			}

			b.ResetTimer()

			var wg sync.WaitGroup
			// 8个读协程
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < b.N; j++ {
						if j%10 == 0 {
							rafts := make([]IRaft, 0, size)
							ll.readHandlers(&rafts)
						} else {
							key := fmt.Sprintf("raft_%d", (id*j)%size)
							ll.get(key)
						}
					}
				}(i)
			}

			// 1个写协程
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < b.N/10; j++ {
					newKey := fmt.Sprintf("raft_temp_%d", j)
					ll.push(&mockRaft{key: newKey})
					ll.remove(newKey)
				}
			}()

			wg.Wait()
		})
	}
}

// 功能测试
func TestLinkedList_Basic(t *testing.T) {
	ll := newLinkedList()

	// 测试 push
	r1 := &mockRaft{key: "raft1"}
	r2 := &mockRaft{key: "raft2"}
	r3 := &mockRaft{key: "raft3"}

	ll.push(r1)
	ll.push(r2)
	ll.push(r3)

	if ll.count() != 3 {
		t.Errorf("Expected count 3, got %d", ll.count())
	}

	// 测试 get
	got1 := ll.get("raft1")
	if got1 == nil || got1.Key() != "raft1" {
		t.Error("get raft1 failed")
	}
	got2 := ll.get("raft2")
	if got2 == nil || got2.Key() != "raft2" {
		t.Error("get raft2 failed")
	}
	if ll.get("nonexistent") != nil {
		t.Error("get nonexistent should return nil")
	}

	// 测试 remove
	ll.remove("raft2")
	if ll.count() != 2 {
		t.Errorf("Expected count 2 after remove, got %d", ll.count())
	}
	if ll.get("raft2") != nil {
		t.Error("raft2 should be removed")
	}

	// 测试 readHandlers
	var rafts []IRaft
	ll.readHandlers(&rafts)
	if len(rafts) != 2 {
		t.Errorf("Expected 2 rafts in readHandlers, got %d", len(rafts))
	}

	// 测试 all
	allRafts := ll.all()
	if len(allRafts) != 2 {
		t.Errorf("Expected 2 rafts in all, got %d", len(allRafts))
	}
}

func TestLinkedList_DuplicatePush(t *testing.T) {
	ll := newLinkedList()
	r1 := &mockRaft{key: "raft1"}

	ll.push(r1)
	ll.push(r1) // 重复 push

	if ll.count() != 1 {
		t.Errorf("Expected count 1 after duplicate push, got %d", ll.count())
	}
}

func TestLinkedList_RemoveNonexistent(t *testing.T) {
	ll := newLinkedList()
	ll.push(&mockRaft{key: "raft1"})

	// 删除不存在的元素
	ll.remove("nonexistent")

	if ll.count() != 1 {
		t.Errorf("Expected count 1, got %d", ll.count())
	}
}

func TestLinkedList_CacheInvalidation(t *testing.T) {
	ll := newLinkedList()

	// 添加元素
	for i := 0; i < 5; i++ {
		ll.push(&mockRaft{key: fmt.Sprintf("raft%d", i)})
	}

	// 第一次 readHandlers，应该构建缓存
	var rafts1 []IRaft
	ll.readHandlers(&rafts1)
	if len(rafts1) != 5 {
		t.Errorf("Expected 5 rafts, got %d", len(rafts1))
	}

	// 第二次 readHandlers，应该使用缓存
	var rafts2 []IRaft
	ll.readHandlers(&rafts2)
	if len(rafts2) != 5 {
		t.Errorf("Expected 5 rafts, got %d", len(rafts2))
	}

	// 添加新元素，应该标记缓存为 dirty
	ll.push(&mockRaft{key: "raft5"})

	// 再次 readHandlers，应该重建缓存
	var rafts3 []IRaft
	ll.readHandlers(&rafts3)
	if len(rafts3) != 6 {
		t.Errorf("Expected 6 rafts after push, got %d", len(rafts3))
	}

	// 删除元素
	ll.remove("raft5")

	// 验证缓存已更新
	var rafts4 []IRaft
	ll.readHandlers(&rafts4)
	if len(rafts4) != 5 {
		t.Errorf("Expected 5 rafts after remove, got %d", len(rafts4))
	}
}
