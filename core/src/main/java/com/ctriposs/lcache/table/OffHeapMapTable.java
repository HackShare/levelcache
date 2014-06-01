package com.ctriposs.lcache.table;

import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 *
 * OffHeap map table accompanied with bloom filter
 *
 * @author bulldog
 *
 */
public class OffHeapMapTable extends AbstractSortedMapTable {
	
	public OffHeapMapTable(int level, long createdTime, int expectedInsertions, long expectedDataOffHeapMemorySize) {
		this((short)0, level, createdTime, expectedInsertions, expectedDataOffHeapMemorySize);
	}
	
	public OffHeapMapTable(short shard, int level, long createdTime, int expectedInsertions, long expectedDataOffHeapMemorySize) {
		super(shard, level, createdTime, expectedInsertions, expectedDataOffHeapMemorySize);
	}

	// for testing
	public IMapEntry appendNew(byte[] key, byte[] value, long timeToLive) {
		return this.appendNew(key, Arrays.hashCode(key), value, timeToLive, System.currentTimeMillis(), false, false);
	}

	@Override
	public IMapEntry appendNew(byte[] key, int keyHash, byte[] value, long timeToLive, long createdTime, boolean markDelete, boolean compressed) {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		Preconditions.checkArgument(this.toAppendIndex.get() < MAX_ALLOWED_NUMBER_OF_ENTRIES,
				"Exceeded max allowed number of entries(" + MAX_ALLOWED_NUMBER_OF_ENTRIES + ")!");

		appendLock.lock();
		try {
			// write index metadata
			byte status = 1; // mark in use
			if (markDelete) {
				status = (byte) (status + 2); // binary 11
			}
			if (compressed && !markDelete) {
				status = (byte) (status + 4);
			}

			
			long offsetInIndexOffHeapMemory = INDEX_ITEM_LENGTH * toAppendIndex.get();
			
			this.indexOffHeapMemory.putLong(offsetInIndexOffHeapMemory, this.toAppendDataOffHeapMemoryOffset.get());
			this.indexOffHeapMemory.putInt(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET, key.length);
			this.indexOffHeapMemory.putInt(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_VALUE_LENGTH_OFFSET, value.length);
			this.indexOffHeapMemory.putLong(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_TIME_TO_LIVE_OFFSET, timeToLive);
			this.indexOffHeapMemory.putLong(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_CREATED_TIME_OFFSET, createdTime);
			this.indexOffHeapMemory.putInt(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET, keyHash);
			this.indexOffHeapMemory.putByte(offsetInIndexOffHeapMemory + IMapEntry.INDEX_ITEM_STATUS, status);

			// write key/value
			this.dataOffHeapMemory.put(toAppendDataOffHeapMemoryOffset.get(), key);
			this.dataOffHeapMemory.put(toAppendDataOffHeapMemoryOffset.get() + key.length, value);
			

			// update guarded condition
			this.bloomFilter.put(key);

			int dataLength = key.length + value.length;
			// commit/update offset & index
			toAppendDataOffHeapMemoryOffset.addAndGet(dataLength);
			int appendedIndex = toAppendIndex.get();
			toAppendIndex.incrementAndGet();
			return new OffHeapMapEntryImpl(appendedIndex, this.indexOffHeapMemory, this.dataOffHeapMemory);
		}
		finally {
			appendLock.unlock();
		}
	}
}
