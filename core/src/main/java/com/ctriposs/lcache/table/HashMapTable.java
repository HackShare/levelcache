package com.ctriposs.lcache.table;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.xerial.snappy.Snappy;

import com.ctriposs.lcache.offheap.OffHeapMemory;
import com.google.common.base.Preconditions;

/**
 * In memory hashmap backed by offheap memory
 *
 * @author bulldog
 *
 */
public class HashMapTable extends AbstractMapTable {

	private AtomicBoolean immutable = new AtomicBoolean(true);

	private ConcurrentHashMap<ByteArrayWrapper, InMemIndex> hashMap;

	private boolean compressionEnabled = true;
	
	public HashMapTable(int level, long createdTime) {
		this((short)0, level, createdTime);
	}

	public HashMapTable(short shard, int level, long createdTime) {
		super(shard, level, createdTime);
		
		this.indexOffHeapMemory = OffHeapMemory.allocateMemory(INIT_INDEX_OFFHEAP_MEMORY_SIZE);
		this.dataOffHeapMemory = OffHeapMemory.allocateMemory(INIT_DATA_OFFHEAP_MEMORY_SIZE);
		
		this.hashMap = new ConcurrentHashMap<ByteArrayWrapper, InMemIndex>(INIT_INDEX_ITEMS_PER_TABLE);
	}

	public void setCompressionEnabled(boolean enabled) {
		this.compressionEnabled = enabled;
	}

	public Set<Map.Entry<ByteArrayWrapper, InMemIndex>> getEntrySet() {
		return this.hashMap.entrySet();
	}

	// for testing
	public IMapEntry appendNew(byte[] key, byte[] value, long timeToLive, long createdTime) {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		return this.appendNew(key, Arrays.hashCode(key), value, timeToLive, createdTime, false, false);
	}

	private IMapEntry appendTombstone(byte[] key) {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		return this.appendNew(key, Arrays.hashCode(key), new byte[] {0}, NO_TIMEOUT, System.currentTimeMillis(), true, false);
	}

	private IMapEntry appendNewCompressed(byte[] key, byte[] value, long timeToLive, long createdTime) {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		return this.appendNew(key, Arrays.hashCode(key), value, timeToLive, createdTime, false, true);
	}

	private IMapEntry appendNew(byte[] key, int keyHash, byte[] value, long timeToLive, long createdTime, boolean markDelete, boolean compressed) {
		
		long tempToAppendIndex;
		long tempToAppendDataOffHeapMemoryOffset;
		
		appendLock.lock();
		try {

			if (toAppendIndex.get() == INIT_INDEX_ITEMS_PER_TABLE) { // index overflow
				return null;
			}
			int dataLength = key.length + value.length;
			if (toAppendDataOffHeapMemoryOffset.get() + dataLength > INIT_DATA_OFFHEAP_MEMORY_SIZE) { // data overflow
				return null;
			}
			
			tempToAppendIndex = toAppendIndex.get();
			tempToAppendDataOffHeapMemoryOffset = toAppendDataOffHeapMemoryOffset.get();

			// commit/update offset & index
			toAppendDataOffHeapMemoryOffset.addAndGet(dataLength);
			toAppendIndex.incrementAndGet();
		}
		finally {
			appendLock.unlock();
		}
		
		byte status = 1; // mark in use
		if (markDelete) {
			status = (byte) (status + 2); // binary 11
		}
		if (compressed && !markDelete) {
			status = (byte) (status + 4);
		}

		// write index
		long offsetInIndexOffHeapMemory = INDEX_ITEM_LENGTH * tempToAppendIndex;
		this.indexOffHeapMemory.putLong(offsetInIndexOffHeapMemory, tempToAppendDataOffHeapMemoryOffset);
		this.indexOffHeapMemory.putInt(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET, key.length);
		this.indexOffHeapMemory.putInt(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_VALUE_LENGTH_OFFSET, value.length);
		this.indexOffHeapMemory.putLong(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_TIME_TO_LIVE_OFFSET, timeToLive);
		this.indexOffHeapMemory.putLong(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_CREATED_TIME_OFFSET, createdTime);
		this.indexOffHeapMemory.putInt(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET, keyHash);
		this.indexOffHeapMemory.putByte(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_STATUS, status);

		// write key/value
		this.dataOffHeapMemory.put(tempToAppendDataOffHeapMemoryOffset, key);
		this.dataOffHeapMemory.put(tempToAppendDataOffHeapMemoryOffset + key.length, value);

		this.hashMap.put(new ByteArrayWrapper(key), new InMemIndex((int)tempToAppendIndex));
		
		return new OffHeapMapEntryImpl((int)tempToAppendIndex, this.indexOffHeapMemory, this.dataOffHeapMemory);
	}

	@Override
	public GetResult get(byte[] key) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		GetResult result = new GetResult();
		InMemIndex inMemIndex = this.hashMap.get(new ByteArrayWrapper(key));
		if (inMemIndex == null) return result;
		
		IMapEntry mapEntry = this.getMapEntry(inMemIndex.getIndex());
		if (mapEntry.isCompressed()) {
			result.setValue(Snappy.uncompress(mapEntry.getValue()));
		} else {
			result.setValue(mapEntry.getValue());
		}
		if (mapEntry.isDeleted()) {
			result.setDeleted(true);
			return result;
		}
		if (mapEntry.isExpired()) {
			result.setExpired(true);
			return result;
		}
		result.setLevel(this.getLevel());
		result.setTimeToLive(mapEntry.getTimeToLive());
		result.setCreatedTime(mapEntry.getCreatedTime());

		return result;
	}

	public void markImmutable(boolean immutable) {
		this.immutable.set(immutable);
	}

	public boolean isImmutable() {
		return this.immutable.get();
	}

	public boolean put(byte[] key, byte[] value, long timeToLive, long createdTime, boolean isDelete) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");

		IMapEntry mapEntry = null;
		if (isDelete) {
			// make a tombstone
			mapEntry = this.appendTombstone(key);
		} else {
			mapEntry = this.compressionEnabled ?
					this.appendNewCompressed(key, Snappy.compress(value), timeToLive, createdTime) : this.appendNew(key, value, timeToLive, createdTime);
		}

		if (mapEntry == null) { // no space
			return false;
		}

		return true;
	}

	public void put(byte[] key, byte[] value, long timeToLive, long createdTime) throws IOException {
		this.put(key, value, timeToLive, createdTime, false);
	}

	public void delete(byte[] key) {
		this.appendTombstone(key);
	}

	public int getRealItemCount() {
		return this.hashMap.size();
	}
}
