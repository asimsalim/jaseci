**ScaleTieredMemory batch_get: full L1 → L2 → L3 read-through**

`ScaleTieredMemory.batch_get()` previously skipped the L2 (Redis) tier and always
fetched L1 misses directly from L3 (MongoDB). The corrected order is:

1. L1 (in-process dict) hit → return immediately
2. L2 Redis `MGET` for L1 misses (single round-trip when `RedisBackend` is active)
3. L3 MongoDB `$in` for L2 misses (single round-trip)
4. L3 hits are promoted to both L1 and L2 so subsequent requests are served from cache

This removes unnecessary MongoDB reads whenever the requested anchors are already
warm in Redis.
