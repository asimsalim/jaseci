"""ScaleTieredMemory Implementation - Multi-tier storage coordination.

Storage configuration comes from environment variables or jac.toml.
"""

"""Initialize ScaleTieredMemory with distributed backends."""
impl ScaleTieredMemory.postinit -> None {
    # L1: Initialize volatile memory (inherited from VolatileMemory via TieredMemory)
    self.__mem__ = {};
    self.__gc__ = set();
    # L2: Try to initialize Redis cache (replaces LocalCacheMemory)
    redis_backend = RedisBackend();
    self._cache_available = redis_backend.is_available();
    if self._cache_available and self.use_cache {
        self.l2 = redis_backend;
        logger.debug("Redis cache backend initialized");
    } else {
        self.l2 = None;
        logger.debug("Redis not available, running without distributed cache");
    }
    # L3: Try MongoDB first (replaces SqliteMemory), fall back to SqliteMemory
    mongo_backend = MongoBackend();
    if mongo_backend.is_available() {
        self.l3 = mongo_backend;
        # Give MongoDB access to L1 memory for sync()
        mongo_backend.set_memory_ref(self.__mem__);
        self._persistence_type = PersistenceType.MONGODB;
        logger.debug("MongoDB persistence backend initialized");
        # Show subtle message (optional - uncomment to enable)
        try {
            import from jaclang.cli.console { console }
            logger.debug("  ✔ Using MongoDB for persistence", style="muted");
        } except Exception { }
    } else {
        # Fall back to jaclang's SqliteMemory using configured path
        self.l3 = SqliteMemory(
            path=get_scale_config().get_database_config()['shelf_db_path']
        );
        self._persistence_type = PersistenceType.SQLITE;
        logger.debug("MongoDB not available, using SqliteMemory for persistence");
        # Show subtle message
        try {
            import from jaclang.cli.console { console }
            logger.debug("  ✔ Using SQLite for persistence", style="muted");
        } except Exception { }
    }
}


"""Commit changes: delegate to L3's sync() which handles change detection
and merge-aware atomic writes."""
impl ScaleTieredMemory.commit(anchor: (Anchor | None) = None) -> None {
    if self.l3 is None {
        return;
    }
    # Both single-anchor and batch commits go through sync() to ensure
    # all writes use merge-aware paths (no direct L3 put bypass).
    # Capture L2-pending anchors BEFORE L3 sync (which updates hashes).
    if self.l3 {
        try {
            l2_pending = [];
            if self.l2 {
                l2_pending = [
                    mem_anchor
                    for (_, mem_anchor) in list(self.__mem__.items())
                    if mem_anchor.persistent
                    and Serializer._compute_hash(mem_anchor) != mem_anchor.hash
                    and Jac.check_write_access(mem_anchor)
                ];
            }
            self.l3.sync();
            if self.l2 {
                for mem_anchor in l2_pending {
                    try {
                        self.l2.put(mem_anchor);
                    } except Exception as l2_err {
                        logger.warning(f"L2 cache update failed: {l2_err}");
                    }
                }
            }
        } except Exception as e {
            logger.warning(f"ScaleTieredMemory.commit sync failed: {e}");
        }
    }
}

"""Batch get: L1 → L2 (Redis mget) → L3 (MongoDB $in) with batch L3→L2 promotion."""
impl ScaleTieredMemory.batch_get(ids: list[UUID]) -> dict[UUID, Anchor] {
    result: dict[UUID, Anchor] = {};
    missing_ids: list[UUID] = [];

    # L1 check (in-process memory, always first)
    for id in ids {
        if (anchor := self.__mem__.get(id)) {
            result[id] = anchor;
        } else {
            missing_ids.append(id);
        }
    }
    if not missing_ids {
        return result;
    }

    # L2 check (Redis mget — single round-trip for all missing ids)
    l3_missing_ids: list[UUID] = list(missing_ids);
    if self.l2 and hasattr(self.l2, 'batch_get') {
        l2_results: dict[UUID, Anchor] = {};
        try {
            l2_results = self.l2.batch_get(missing_ids);
        } except Exception as e {
            logger.debug(f"L2 batch_get failed, falling through to L3: {e}");
        }
        if l2_results {
            l3_missing_ids = [];
            for id in missing_ids {
                if id in l2_results {
                    a2 = l2_results[id];
                    self.__mem__[id] = a2;
                    result[id] = a2;
                } else {
                    l3_missing_ids.append(id);
                }
            }
        }
    }

    # L3 check (MongoDB $in query — single round-trip for all remaining ids)
    if l3_missing_ids and self.l3 {
        l3_results: dict[UUID, Anchor] = {};
        if hasattr(self.l3, 'batch_get') {
            l3_results = self.l3.batch_get(l3_missing_ids);
        } else {
            for mid in l3_missing_ids {
                if (a := self.l3.get(mid)) {
                    l3_results[mid] = a;
                }
            }
        }
        if l3_results {
            # Promote all L3 hits to L2 in a single pipeline when available
            if self.l2 {
                try {
                    if hasattr(self.l2, 'batch_put') {
                        self.l2.batch_put(list(l3_results.values()));
                    } else {
                        for a3 in l3_results.values() {
                            self.l2.put(a3);
                        }
                    }
                } except Exception as e {
                    logger.debug(f"L2 batch promotion failed: {e}");
                }
            }
            for (id, anchor) in l3_results.items() {
                self.__mem__[id] = anchor;
                result[id] = anchor;
            }
        }
    }
    return result;
}

"""Close all backends."""
impl ScaleTieredMemory.close -> None {
    self.commit();
    if self.l3 {
        self.l3.close();
    }
    # Close L2 cache
    if self.l2 {
        self.l2.close();
    }
    # Clear L1 (inherited from VolatileMemory)
    self.__mem__.clear();
    self.__gc__.clear();
}
