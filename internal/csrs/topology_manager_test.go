package csrs

import (
	"fmt"
	"github.com/let-mil-go/internal/hlc"
	"sync"
	"testing"
)

func TestShardRegistry_RegisterAndGet(t *testing.T) {
	registry := NewShardRegistry()

	info := &ShardInfo{
		ShardID:    "shard0001",
		Address:    "192.168.10.11:27018",
		ReplicaSet: "rs-shard1",
		Priority:   1,
		Status:     ShardStatusActive,
	}

	isNew := registry.Register(info)
	if !isNew {
		t.Error("expected Register to return true for new shard")
	}

	got := registry.Get("shard0001")
	if got == nil {
		t.Fatal("expected to get shard info")
	}
	if got.Address != "192.168.10.11:27018" {
		t.Errorf("expected address 192.168.10.11:27018, got %s", got.Address)
	}

	isNew = registry.Register(info)
	if isNew {
		t.Error("expected Register to return false for existing shard")
	}
}

func TestShardRegistry_Unregister(t *testing.T) {
	registry := NewShardRegistry()

	info := &ShardInfo{
		ShardID: "shard0001",
		Address: "192.168.10.11:27018",
		Status:  ShardStatusActive,
	}
	registry.Register(info)

	removed := registry.Unregister("shard0001")
	if removed == nil {
		t.Error("expected Unregister to return removed info")
	}

	if registry.Get("shard0001") != nil {
		t.Error("expected shard to be removed")
	}

	removed = registry.Unregister("nonexistent")
	if removed != nil {
		t.Error("expected Unregister to return nil for nonexistent shard")
	}
}

func TestShardRegistry_ConcurrentAccess(t *testing.T) {
	registry := NewShardRegistry()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			info := &ShardInfo{
				ShardID: fmt.Sprintf("shard%04d", idx),
				Address: fmt.Sprintf("192.168.10.%d:27018", idx),
				Status:  ShardStatusActive,
			}
			registry.Register(info)
		}(i)
	}
	wg.Wait()

	if registry.Count() != 100 {
		t.Errorf("expected 100 shards, got %d", registry.Count())
	}
}

func TestHashRing_BasicRouting(t *testing.T) {
	ring := NewHashRing()

	ring.AddShard("shard0001")
	ring.AddShard("shard0002")
	ring.AddShard("shard0003")

	if ring.ShardCount() != 3 {
		t.Errorf("expected 3 shards, got %d", ring.ShardCount())
	}

	shard := ring.GetShardForKey("testkey")
	if shard == "" {
		t.Error("expected non-empty shard for key")
	}

	shard2 := ring.GetShardForKey("testkey")
	if shard != shard2 {
		t.Errorf("expected consistent routing, got %s and %s", shard, shard2)
	}
}

func TestHashRing_KeyDistribution(t *testing.T) {
	ring := NewHashRing()

	ring.AddShard("shard0001")
	ring.AddShard("shard0002")
	ring.AddShard("shard0003")

	distribution := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		shard := ring.GetShardForKey(key)
		distribution[shard]++
	}

	for shard, count := range distribution {
		ratio := float64(count) / 10000.0
		if ratio < 0.2 || ratio > 0.5 {
			t.Errorf("shard %s has uneven distribution: %.2f%%", shard, ratio*100)
		}
	}
}

func TestHashRing_RemoveShard(t *testing.T) {
	ring := NewHashRing()

	ring.AddShard("shard0001")
	ring.AddShard("shard0002")
	ring.AddShard("shard0003")

	initialAssignments := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		initialAssignments[key] = ring.GetShardForKey(key)
	}

	ring.RemoveShard("shard0002")

	if ring.ShardCount() != 2 {
		t.Errorf("expected 2 shards after removal, got %d", ring.ShardCount())
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		shard := ring.GetShardForKey(key)
		if shard == "shard0002" {
			t.Errorf("key %s still assigned to removed shard", key)
		}
	}
}

func TestHashRing_GetNShards(t *testing.T) {
	ring := NewHashRing()

	ring.AddShard("shard0001")
	ring.AddShard("shard0002")
	ring.AddShard("shard0003")

	shards := ring.GetNShards("testkey", 2)
	if len(shards) != 2 {
		t.Errorf("expected 2 shards, got %d", len(shards))
	}

	if shards[0] == shards[1] {
		t.Error("expected distinct shards")
	}

	shards = ring.GetNShards("testkey", 5)
	if len(shards) != 3 {
		t.Errorf("expected 3 shards (all available), got %d", len(shards))
	}
}

func TestTopologyManager_AddAndRemoveShard(t *testing.T) {
	clock := hlc.NewWithNowFn(func() uint64 { return 1000 })
	tm := NewTopologyManagerWithClock(clock)

	info := &ShardInfo{
		ShardID: "shard0001",
		Address: "192.168.10.11:27018",
		Status:  ShardStatusActive,
	}

	v1, err := tm.AddShard(info)
	if err != nil {
		t.Fatalf("failed to add shard: %v", err)
	}
	if v1 == 0 {
		t.Error("expected non-zero topology version")
	}

	if tm.ShardCount() != 1 {
		t.Errorf("expected 1 shard, got %d", tm.ShardCount())
	}

	v2, err := tm.RemoveShard("shard0001")
	if err != nil {
		t.Fatalf("failed to remove shard: %v", err)
	}
	if v2 <= v1 {
		t.Errorf("expected version to increase after removal, got v1=%d v2=%d", v1, v2)
	}
}

func TestTopologyManager_GetShardForKey(t *testing.T) {
	tm := NewTopologyManager()

	shards := []struct {
		id   string
		addr string
	}{
		{"shard0001", "192.168.10.11:27018"},
		{"shard0002", "192.168.10.12:27019"},
		{"shard0003", "192.168.10.13:27020"},
	}

	for _, s := range shards {
		info := &ShardInfo{
			ShardID: s.id,
			Address: s.addr,
			Status:  ShardStatusActive,
		}
		tm.AddShard(info)
	}

	shardID, err := tm.GetShardForKey("user:12345")
	if err != nil {
		t.Fatalf("failed to get shard for key: %v", err)
	}
	if shardID == "" {
		t.Error("expected non-empty shard ID")
	}

	addr, err := tm.GetShardAddressForKey("user:12345")
	if err != nil {
		t.Fatalf("failed to get shard address: %v", err)
	}
	if addr == "" {
		t.Error("expected non-empty address")
	}
}

func TestTopologyManager_VersionIncrement(t *testing.T) {
	clock := hlc.NewWithNowFn(func() uint64 { return 1000 })
	tm := NewTopologyManagerWithClock(clock)

	initialVersion := tm.GetTopologyVersion()
	if initialVersion != 0 {
		t.Errorf("expected initial version 0, got %d", initialVersion)
	}

	info := &ShardInfo{
		ShardID: "shard0001",
		Address: "192.168.10.11:27018",
		Status:  ShardStatusActive,
	}
	v1, _ := tm.AddShard(info)

	info2 := &ShardInfo{
		ShardID: "shard0002",
		Address: "192.168.10.12:27019",
		Status:  ShardStatusActive,
	}
	v2, _ := tm.AddShard(info2)

	if v2 <= v1 {
		t.Errorf("expected version to increase, got v1=%d v2=%d", v1, v2)
	}

	v3, _ := tm.UpdateShardStatus("shard0001", ShardStatusDraining)
	if v3 <= v2 {
		t.Errorf("expected version to increase after status update, got v2=%d v3=%d", v2, v3)
	}
}

func TestTopologyManager_StaleVersionCheck(t *testing.T) {
	tm := NewTopologyManager()

	info := &ShardInfo{
		ShardID: "shard0001",
		Address: "192.168.10.11:27018",
		Status:  ShardStatusActive,
	}
	v1, _ := tm.AddShard(info)

	if tm.IsVersionStale(v1) {
		t.Error("current version should not be stale")
	}

	info2 := &ShardInfo{
		ShardID: "shard0002",
		Address: "192.168.10.12:27019",
		Status:  ShardStatusActive,
	}
	tm.AddShard(info2)

	if !tm.IsVersionStale(v1) {
		t.Error("old version should be stale")
	}
}

func TestTopologyManager_GetTopologySnapshot(t *testing.T) {
	tm := NewTopologyManager()

	shards := []struct {
		id   string
		addr string
	}{
		{"shard0001", "192.168.10.11:27018"},
		{"shard0002", "192.168.10.12:27019"},
	}

	for _, s := range shards {
		info := &ShardInfo{
			ShardID: s.id,
			Address: s.addr,
			Status:  ShardStatusActive,
		}
		tm.AddShard(info)
	}

	snapshot := tm.GetTopologySnapshot()

	if snapshot.Version == 0 {
		t.Error("expected non-zero snapshot version")
	}
	if len(snapshot.Shards) != 2 {
		t.Errorf("expected 2 shards in snapshot, got %d", len(snapshot.Shards))
	}
}

func TestTopologyManager_EmptyRing(t *testing.T) {
	tm := NewTopologyManager()

	_, err := tm.GetShardForKey("testkey")
	if err == nil {
		t.Error("expected error for empty ring")
	}
}

func TestTopologyManager_ValidationErrors(t *testing.T) {
	tm := NewTopologyManager()

	_, err := tm.AddShard(&ShardInfo{ShardID: "", Address: "192.168.10.11:27018"})
	if err == nil {
		t.Error("expected error for empty shard ID")
	}

	_, err = tm.AddShard(&ShardInfo{ShardID: "shard0001", Address: ""})
	if err == nil {
		t.Error("expected error for empty address")
	}

	_, err = tm.RemoveShard("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent shard")
	}
}
