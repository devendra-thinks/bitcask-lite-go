package main

import "sync"

// mp defines a concurrent map of 128 shards
// Access a shard to take a lock on correct shard
// provides functionality to set, get , delete keys
type mp[T any] []*MapShard[T]

const SHARDS = 128

type MapShard[T any] struct {
	// map of each shard 
	items map[string]T
	// mutex to access each shard 
	mu *sync.Mutex
}

//create a map 
func CreateMap[T any]() *mp[T]{
    mp := make(mp[T], SHARDS)
    for i := 0 ; i < SHARDS ; i++{
		 mp[i] = &MapShard[T]{items : make(map[string]T), mu : &sync.Mutex{}}
	}
	return &mp
}

// hash a key 
// fnv - 32 
// TODO - read more about hashing functions
func hash(key string) uint32{
    hsh :=  uint32(2166136261)
	const prime32 = uint32(16777619)
	len := len(key)
	for i := 0 ; i < len ; i++ {
		hsh = hsh*prime32
		hsh = hsh ^ uint32(key[i])
	}
	return hsh
}


// find shard for given key
func (mp mp[V])getShard(key string ) *MapShard[V] {
	hshK := hash(key)
	return mp[uint(hshK)%uint(SHARDS)]
}

// access a shard 
func (mp mp[V]) AccessShard(key string ) *sync.Mutex{
	 shard := mp.getShard(key)
	 shard.mu.Lock()
     return shard.mu
}

// sets a key into shard
func (mp mp[V]) SetKey(key string , value V){
	 shard := *mp.getShard(key)
     shard.items[key] = value 
}

// gets a key from shard 
func (mp mp[V]) GetKey(key string)(V, bool){
	 shard := *mp.getShard(key)
	 v, ok := shard.items[key]
	 return v, ok
}

// deletes a key 
func (mp mp[V]) DeleteKey(key string){
	 shard := *mp.getShard(key)
	 delete(shard.items, key)
}

// merge keys 
func (m mp[V]) MSet(data map[string]V) {
	for key, value := range data {
		shard := m.getShard(key)
		shard.items[key] = value
	}
}