"""MongoDB PersistenceBackend Implementation."""

"""Post-initialization: connect to MongoDB."""
impl MongoBackend.postinit -> None {
    if self.mongo_url is None {
        self.mongo_url = _get_db_config().get('mongodb_uri');
    }
    if self.mongo_url {
        if 'mongo_client' not in _process_cache {
            try {
                _process_cache['mongo_client'] = MongoClient(self.mongo_url);
            } except Exception as e {
                logger.warning(f"MongoDB connection failed: {e}");
            }
        }
        self.client = _process_cache.get('mongo_client');
        if self.client {
            self.db = self.client[self.db_name];
            self.collection = self.db[self.collection_name];
        }
    }
}

"""Set reference to parent TieredMemory's __mem__ for sync()."""
impl MongoBackend.set_memory_ref(mem: dict[UUID, Anchor]) -> None {
    self._mem_ref = mem;
}

"""Check if MongoDB is available and connected."""
impl MongoBackend.is_available -> bool {
    if 'mongo_available' in _process_cache {
        return _process_cache['mongo_available'];
    }
    if not self.mongo_url {
        self.mongo_url = _get_db_config().get('mongodb_uri');
    }
    if not self.mongo_url or self.client is None {
        _process_cache['mongo_available'] = False;
        return False;
    }
    try {
        self.client.admin.command('ping', maxTimeMS=100);
        _process_cache['mongo_available'] = True;
        return True;
    } except ConnectionFailure {
        _process_cache['mongo_available'] = False;
        return False;
    } except Exception as e {
        logger.debug(f"MongoDB availability check failed: {e}");
        _process_cache['mongo_available'] = False;
        return False;
    }
}

"""Get anchor by UUID from MongoDB."""
impl MongoBackend.get(id: UUID) -> (Anchor | None) {
    if self.client is None {
        return None;
    }
    _id = to_uuid(id);
    try {
        db_obj = self.collection.find_one({'_id': str(_id)});
        if db_obj {
            anchor = self._load_anchor(db_obj);
            if anchor {
                stored_hash = db_obj.get('_hash');
                if stored_hash is not None {
                    anchor.hash = stored_hash;
                }
            }
            return anchor;
        }
    } except Exception as e {
        logger.debug(f"MongoDB get failed: {e}");
    }
    return None;
}

"""Store anchor in MongoDB with CAS to prevent last-writer-wins overwrites."""
impl MongoBackend.put(anchor: Anchor) -> None {
    if self.client is None or not anchor.persistent {
        return;
    }
    _id = to_uuid(anchor.id);
    try {
        data = Serializer.serialize(anchor, include_type=True);
        new_hash = Serializer._compute_hash(anchor);
        old_hash = anchor.hash;
        if old_hash is not None {
            result = self.collection.update_one(
                {'_id': str(_id), '_hash': old_hash},
                {'$set': {'data': data, 'type': type(anchor).__name__, '_hash': new_hash}}
            );
            if result.matched_count == 0 {
                logger.warning(
                    f"CAS conflict on anchor {_id}: concurrent modification detected, discarding stale update"
                );
                return;
            }
        } else {
            self.collection.update_one(
                {'_id': str(_id)},
                {'$set': {'data': data, 'type': type(anchor).__name__, '_hash': new_hash}},
                upsert=True
            );
        }
        anchor.hash = new_hash;
    } except Exception as e {
        logger.error(f"MongoDB put failed: {e}");
    }
}

"""Delete anchor from MongoDB."""
impl MongoBackend.delete(id: UUID) -> None {
    if self.client is None {
        return;
    }
    _id = to_uuid(id);
    try {
        self.collection.delete_one({'_id': str(_id)});
    } except Exception as e {
        logger.debug(f"MongoDB delete failed: {e}");
    }
}

"""Close MongoDB connection."""
impl MongoBackend.close -> None {
    # Don't close the shared process-level client; just drop local reference.
    self.client = None;
    self._mem_ref = None;
}

"""Check if an anchor exists in MongoDB."""
impl MongoBackend.`has(id: UUID) -> bool {
    if self.client is None {
        return False;
    }
    _id = to_uuid(id);
    try {
        return self.collection.count_documents({'_id': str(_id)}, limit=1) > 0;
    } except Exception {
        return False;
    }
}

"""Query all anchors with optional filter."""
impl MongoBackend.query(
    filter: (Callable[[Anchor], bool] | None) = None
) -> Generator[Anchor, None, None] {
    if self.client is None {
        return;
    }
    try {
        for doc in self.collection.find() {
            if (anchor := self._load_anchor(doc)) {
                if filter is None or filter(anchor) {
                    yield anchor;
                }
            }
        }
    } except Exception as e {
        logger.debug(f"MongoDB query failed: {e}");
    }
}

"""Get all root anchors."""
impl MongoBackend.get_roots -> Generator[Root, None, None] {
    for anchor in self.query() {
        if isinstance(anchor.archetype, Root) {
            yield cast(Root, anchor.archetype);
        }
    }
}

"""Find anchors by IDs with optional filter."""
impl MongoBackend.find(
    ids: (UUID | Iterable[UUID]), filter: (Callable[[Anchor], Anchor] | None) = None
) -> Generator[Anchor, None, None] {
    id_list = [ids] if isinstance(ids, UUID) else list(ids);
    for id in id_list {
        if (anchor := self.get(id)) {
            if filter is None or filter(anchor) {
                yield anchor;
            }
        }
    }
}

"""Find one anchor by ID(s) with optional filter."""
impl MongoBackend.find_one(
    ids: (UUID | Iterable[UUID]), filter: (Callable[[Anchor], Anchor] | None) = None
) -> (Anchor | None) {
    id_list = [ids] if isinstance(ids, UUID) else list(ids);
    for id in id_list {
        if (anchor := self.get(id)) {
            if filter is None or filter(anchor) {
                return anchor;
            }
        }
    }
    return None;
}

"""Commit - no-op for MongoDB (writes are immediate)."""
impl MongoBackend.commit(anchor: (Anchor | None) = None) -> None {
# No-op: MongoDB writes are immediate
}

# PersistentMemory-specific methods
"""Sync - flush L1 memory to MongoDB using bulk_write (single round-trip)."""
impl MongoBackend.sync -> None {
    import from jaclang { JacRuntimeInterface as Jac }
    if not self._mem_ref or not self.client {
        return;
    }
    dirty: list = [];
    for (_, anchor) in list(self._mem_ref.items()) {
        if not anchor.persistent {
            continue;
        }
        current_hash = Serializer._compute_hash(anchor);
        if current_hash == anchor.hash {
            continue;
        }
        if not Jac.check_write_access(anchor) {
            continue;
        }
        dirty.append(anchor);
    }
    if dirty {
        try {
            self.bulk_put(dirty);
            for anchor in dirty {
                anchor.hash = Serializer._compute_hash(anchor);
            }
        } except Exception as e {
            logger.warning(f"MongoDB sync bulk_put failed: {e}");
        }
    }
}

"""Bulk store multiple anchors with CAS for existing documents."""
impl MongoBackend.bulk_put(anchors: Iterable[Anchor]) -> None {
    if self.client is None {
        return;
    }
    ops: list = [];
    for anchor in anchors {
        if not anchor.persistent {
            continue;
        }
        _id = to_uuid(anchor.id);
        try {
            data = Serializer.serialize(anchor, include_type=True);
            new_hash = Serializer._compute_hash(anchor);
            old_hash = anchor.hash;
            if old_hash {
                # CAS: only overwrite if stored hash matches our last-read version
                ops.append(
                    UpdateOne(
                        {'_id': str(_id), '_hash': old_hash},
                        {'$set': {'data': data, 'type': type(anchor).__name__, '_hash': new_hash}}
                    )
                );
            } else {
                ops.append(
                    UpdateOne(
                        {'_id': str(_id)},
                        {'$set': {'data': data, 'type': type(anchor).__name__, '_hash': new_hash}},
                        upsert=True
                    )
                );
            }
        } except Exception as e {
            logger.debug(f"MongoDB bulk_put serialization failed: {e}");
        }
    }
    if ops {
        try {
            self.collection.bulk_write(ops, ordered=False);
        } except Exception as e {
            logger.debug(f"MongoDB bulk_write failed: {e}");
        }
    }
}

"""Batch get multiple anchors by ID using MongoDB $in query."""
impl MongoBackend.batch_get(ids: list[UUID]) -> dict[UUID, Anchor] {
    result: dict[UUID, Anchor] = {};
    if self.client is None or not ids {
        return result;
    }
    str_ids = [str(to_uuid(id)) for id in ids];
    try {
        for doc in self.collection.find({'_id': {'$in': str_ids}}) {
            if (anchor := self._load_anchor(doc)) {
                anchor.is_updated = False;
                stored_hash = doc.get('_hash');
                if stored_hash is not None {
                    anchor.hash = stored_hash;
                }
                result[UUID(doc['_id'])] = anchor;
            }
        }
    } except Exception as e {
        logger.debug(f"MongoDB batch_get failed: {e}");
    }
    return result;
}

"""Load anchor from raw MongoDB document."""
impl MongoBackend._load_anchor(raw: dict[(str, Any)]) -> (Anchor | None) {
    if 'data' not in raw {
        return None;
    }
    try {
        data = raw['data'];
        return Serializer.deserialize(data);
    } except Exception as e {
        doc_id = raw.get('_id', '<unknown>');
        doc_type = raw.get('type', '<unknown>');
        logger.warning(
            f"MongoDB _load_anchor failed to deserialize anchor "
            f"(id={doc_id}, type={doc_type}): {e}. "
            f"This anchor will be skipped. Data may be from an incompatible "
            f"version or the class definition has changed."
        );
        return None;
    }
}
