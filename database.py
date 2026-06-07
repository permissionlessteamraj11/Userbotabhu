import asyncio
import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        return super().default(obj)

def datetime_decoder(dct):
    if "__type__" in dct and dct["__type__"] == "datetime":
        return datetime.fromisoformat(dct["value"])
    return dct

class JSONCursor:
    def __init__(self, data: List[Dict[str, Any]]):
        self._data = data
        self._pos = 0

    def skip(self, n: int) -> 'JSONCursor':
        self._data = self._data[n:]
        return self

    def limit(self, n: int) -> 'JSONCursor':
        self._data = self._data[:n]
        return self

    async def to_list(self, length: Optional[int]) -> List[Dict[str, Any]]:
        if length is None:
            return self._data
        return self._data[:length]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._pos >= len(self._data):
            raise StopAsyncIteration
        res = self._data[self._pos]
        self._pos += 1
        return res

class JSONCollection:
    def __init__(self, filepath: str, lock: asyncio.Lock):
        self.filepath = filepath
        self.lock = lock

    async def _read(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.filepath):
            return []
        return await asyncio.to_thread(self._read_sync)

    def _read_sync(self) -> List[Dict[str, Any]]:
        try:
            with open(self.filepath, "r") as f:
                return json.load(f, object_hook=datetime_decoder)
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    async def _write(self, data: List[Dict[str, Any]]):
        await asyncio.to_thread(self._write_sync, data)

    def _write_sync(self, data: List[Dict[str, Any]]):
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump(data, f, cls=DateTimeEncoder, indent=4)

    def _match(self, doc: Dict[str, Any], filter: Dict[str, Any]) -> bool:
        for k, v in filter.items():
            if k not in doc or doc[k] != v:
                return False
        return True

    async def find_one(self, filter: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        filter = filter or {}
        async with self.lock:
            data = await self._read()
            for doc in data:
                if self._match(doc, filter):
                    return doc
        return None

    def find(self, filter: Dict[str, Any] = None) -> JSONCursor:
        filter = filter or {}
        # We need to read data but find() in motor is not async,
        # so we'll have to handle it carefully.
        # Actually, in motor, find() returns a cursor and you await to_list or iterate.
        # So we can do the filtering when to_list or __aiter__ is called.
        # But wait, JSONCursor needs the data.
        # Let's make a LazyJSONCursor.
        return LazyJSONCursor(self, filter)

    async def insert_one(self, doc: Dict[str, Any]):
        async with self.lock:
            data = await self._read()
            if "_id" not in doc:
                doc["_id"] = str(uuid.uuid4())
            data.append(doc)
            await self._write(data)
            class InsertResult:
                def __init__(self, id):
                    self.inserted_id = id
            return InsertResult(doc["_id"])

    async def update_one(self, filter: Dict[str, Any], update: Dict[str, Any], upsert: bool = False):
        async with self.lock:
            data = await self._read()
            target = None
            for doc in data:
                if self._match(doc, filter):
                    target = doc
                    break

            if target is None:
                if upsert:
                    new_doc = filter.copy()
                    if "$set" in update:
                        new_doc.update(update["$set"])
                    if "$setOnInsert" in update:
                        new_doc.update(update["$setOnInsert"])
                    # $inc is tricky on upsert, usually starts from 0
                    if "$inc" in update:
                        for k, v in update["$inc"].items():
                            new_doc[k] = v
                    if "_id" not in new_doc:
                        new_doc["_id"] = str(uuid.uuid4())
                    data.append(new_doc)
                    await self._write(data)
                return

            if "$set" in update:
                for k, v in update["$set"].items():
                    # Handle nested set like "stats.sent"
                    if "." in k:
                        parts = k.split(".")
                        d = target
                        for p in parts[:-1]:
                            d = d.setdefault(p, {})
                        d[parts[-1]] = v
                    else:
                        target[k] = v

            if "$inc" in update:
                for k, v in update["$inc"].items():
                    if "." in k:
                        parts = k.split(".")
                        d = target
                        for p in parts[:-1]:
                            d = d.setdefault(p, {})
                        d[parts[-1]] = d.get(parts[-1], 0) + v
                    else:
                        target[k] = target.get(k, 0) + v

            await self._write(data)

    async def delete_one(self, filter: Dict[str, Any]):
        async with self.lock:
            data = await self._read()
            for i, doc in enumerate(data):
                if self._match(doc, filter):
                    data.pop(i)
                    break
            await self._write(data)

    async def count_documents(self, filter: Dict[str, Any]) -> int:
        async with self.lock:
            data = await self._read()
            count = 0
            for doc in data:
                if self._match(doc, filter):
                    count += 1
            return count

    async def distinct(self, field: str) -> List[Any]:
        async with self.lock:
            data = await self._read()
            res = set()
            for doc in data:
                if field in doc:
                    res.add(doc[field])
            return list(res)

    async def aggregate(self, pipeline: List[Dict[str, Any]]) -> JSONCursor:
        # We only need to support:
        # [{"$group": {"_id": None, "ts": {"$sum": "$sent"}, "tf": {"$sum": "$failed"}}}]
        async with self.lock:
            data = await self._read()

            for stage in pipeline:
                if "$group" in stage:
                    group = stage["$group"]
                    if group.get("_id") is None:
                        result = {"_id": None}
                        for k, v in group.items():
                            if k == "_id": continue
                            if isinstance(v, dict) and "$sum" in v:
                                field = v["$sum"]
                                if field.startswith("$"):
                                    field = field[1:]
                                    result[k] = sum(doc.get(field, 0) for doc in data)
                        return JSONCursor([result])
            return JSONCursor([])

class LazyJSONCursor:
    def __init__(self, collection: JSONCollection, filter: Dict[str, Any]):
        self.collection = collection
        self.filter = filter
        self._skip = 0
        self._limit = None

    def skip(self, n: int) -> 'LazyJSONCursor':
        self._skip = n
        return self

    def limit(self, n: int) -> 'LazyJSONCursor':
        self._limit = n
        return self

    async def _get_data(self) -> List[Dict[str, Any]]:
        async with self.collection.lock:
            data = await self.collection._read()
            filtered = [doc for doc in data if self.collection._match(doc, self.filter)]
            if self._skip:
                filtered = filtered[self._skip:]
            if self._limit is not None:
                filtered = filtered[:self._limit]
            return filtered

    async def to_list(self, length: Optional[int]) -> List[Dict[str, Any]]:
        data = await self._get_data()
        if length is not None:
            return data[:length]
        return data

    def __aiter__(self):
        return self

    async def __anext__(self):
        # This is a bit inefficient for LazyJSONCursor as it fetches all data every time if not careful
        # But for this bot it should be fine.
        # Better: fetch once.
        if not hasattr(self, "_cached_data"):
            self._cached_data = await self._get_data()
            self._pos = 0

        if self._pos >= len(self._cached_data):
            raise StopAsyncIteration
        res = self._cached_data[self._pos]
        self._pos += 1
        return res

class JSONDatabase:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        self.locks = {}

    def __getitem__(self, name: str) -> JSONCollection:
        if name not in self.locks:
            self.locks[name] = asyncio.Lock()
        filepath = os.path.join(self.base_dir, f"{name}.json")
        return JSONCollection(filepath, self.locks[name])
