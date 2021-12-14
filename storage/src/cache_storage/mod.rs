// Copyright (c) The Starcoin Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::batch::WriteBatch;
use crate::metrics::{record_metrics, StorageMetrics};
use crate::storage::{InnerStore, WriteOp};
use anyhow::{Error, Result};
use lru::LruCache;
use parking_lot::Mutex;
use starcoin_config::DEFAULT_CACHE_SIZE;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

static NUM_SHARD_BITS: usize = 4;
static NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;
pub struct ShardLruCache {
    caches: Vec<Mutex<LruCache<Vec<u8>, Vec<u8>>>>,
}

impl ShardLruCache {
    pub fn new(cap: usize) -> Self {
        let per_shard_cap = (cap + NUM_SHARDS - 1) / NUM_SHARDS;
        Self {
            caches: (0..NUM_SHARDS).map(|_|Mutex::new(LruCache::new(per_shard_cap))).collect(),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        let idx = ShardLruCache::get_idx(&key);
        self.caches[idx].lock().put(key, value)
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let idx = ShardLruCache::get_idx(key);
        self.caches[idx].lock().get(key).cloned()
    }

    pub fn pop(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        let idx = ShardLruCache::get_idx(key);
        self.caches[idx].lock().pop(key)
    }

    pub fn len(&self) -> usize {
        self.caches.iter().fold(0, |x, obj| obj.lock().len() + x)
    }

    pub fn contains(&self, key: &Vec<u8>) -> bool {
       let idx = ShardLruCache::get_idx(key);
        self.caches[idx].lock().contains(key)
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut all_keys = vec![];
        for cache in &self.caches {
            for (key, _) in cache.lock().iter() {
                all_keys.push(key.to_vec());
            }
        }
        all_keys
    }

    fn shard(hash: u32) -> u32 {
        hash >> (32 - NUM_SHARD_BITS)
    }

    fn get_idx(key: &Vec<u8>) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        ShardLruCache::shard(hasher.finish() as u32) as usize
    }
}

pub struct CacheStorage {
    cache: ShardLruCache,
    metrics: Option<StorageMetrics>,
}

impl CacheStorage {
    pub fn new(metrics: Option<StorageMetrics>) -> Self {
        CacheStorage {
            cache: ShardLruCache::new(DEFAULT_CACHE_SIZE),
            metrics,
        }
    }
    pub fn new_with_capacity(size: usize, metrics: Option<StorageMetrics>) -> Self {
        CacheStorage {
            cache: ShardLruCache::new(size),
            metrics,
        }
    }
}

impl Default for CacheStorage {
    fn default() -> Self {
        Self::new(None)
    }
}

impl InnerStore for CacheStorage {
    fn get(&self, prefix_name: &str, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        record_metrics("cache", prefix_name, "get", self.metrics.as_ref()).call(|| {
            Ok(self
                .cache
                .get(&compose_key(prefix_name.to_string(), key)))
        })
    }

    fn put(&self, prefix_name: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // remove record_metrics for performance
        // record_metrics add in write_batch to reduce Instant::now system call
        self.cache.put(compose_key(prefix_name.to_string(), key), value);
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.cache_items.set(self.cache.len() as u64);
        }
        Ok(())
    }

    fn contains_key(&self, prefix_name: &str, key: Vec<u8>) -> Result<bool> {
        record_metrics("cache", prefix_name, "contains_key", self.metrics.as_ref()).call(|| {
            Ok(self
                .cache
                .contains(&compose_key(prefix_name.to_string(), key)))
        })
    }
    fn remove(&self, prefix_name: &str, key: Vec<u8>) -> Result<()> {
        // remove record_metrics for performance
        // record_metrics add in write_batch to reduce Instant::now system call
        self.cache.pop(&compose_key(prefix_name.to_string(), key));
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.cache_items.set(self.cache.len() as u64);
        }
        Ok(())
    }

    fn write_batch(&self, prefix_name: &str, batch: WriteBatch) -> Result<()> {
        record_metrics("cache", prefix_name, "write_batch", self.metrics.as_ref()).call(|| {
            for (key, write_op) in &batch.rows {
                match write_op {
                    WriteOp::Value(value) => self.put(prefix_name, key.to_vec(), value.to_vec())?,
                    WriteOp::Deletion => self.remove(prefix_name, key.to_vec())?,
                };
            }
            Ok(())
        })
    }

    fn get_len(&self) -> Result<u64, Error> {
        Ok(self.cache.len() as u64)
    }

    fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self.cache.keys())
    }

    fn put_sync(&self, prefix_name: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.put(prefix_name, key, value)
    }

    fn write_batch_sync(&self, prefix_name: &str, batch: WriteBatch) -> Result<()> {
        self.write_batch(prefix_name, batch)
    }
}

fn compose_key(prefix_name: String, source_key: Vec<u8>) -> Vec<u8> {
    let temp_vec = prefix_name.as_bytes().to_vec();
    let mut compose = Vec::with_capacity(temp_vec.len() + source_key.len());
    compose.extend(temp_vec);
    compose.extend(source_key);
    compose
}
