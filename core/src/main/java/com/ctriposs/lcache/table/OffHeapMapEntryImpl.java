package com.ctriposs.lcache.table;

import com.ctriposs.lcache.offheap.OffHeapMemory;

/**
 * OffHeap memory map entry implementation
 * 
 * @author bulldog
 *
 */
public class OffHeapMapEntryImpl implements IMapEntry {
	
	private int index;
	
	private OffHeapMemory dataOffHeapMemory;
	private OffHeapMemory indexOffHeapMemory;
	
	public OffHeapMapEntryImpl(int index, OffHeapMemory indexOffHeapMemory, OffHeapMemory dataOffHeapMemory) {
		this.index = index;
		this.indexOffHeapMemory = indexOffHeapMemory;
		this.dataOffHeapMemory = dataOffHeapMemory;
	}
	
	int getKeyLength() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_KEY_LENGTH_OFFSET;
		return this.indexOffHeapMemory.getInt(offsetInIndexOffHeapMemory);
	}
	
	int getValueLength() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_VALUE_LENGTH_OFFSET;
		return this.indexOffHeapMemory.getInt(offsetInIndexOffHeapMemory);
	}
	
	long getItemOffsetInDataOffHeapMemory() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index;
		return this.indexOffHeapMemory.getLong(offsetInIndexOffHeapMemory);
	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public byte[] getKey() {
		long itemOffsetInDataOffHeapMemory = this.getItemOffsetInDataOffHeapMemory();
		int keyLength = this.getKeyLength();
		byte[] result = new byte[keyLength];
		this.dataOffHeapMemory.get(itemOffsetInDataOffHeapMemory, result);
		return result;
	}

	@Override
	public byte[] getValue() {
		long itemOffsetInDataOffHeapMemory = this.getItemOffsetInDataOffHeapMemory();
		int keyLength = this.getKeyLength();
		itemOffsetInDataOffHeapMemory += keyLength;
		int valueLength = this.getValueLength();
		byte[] result = new byte[valueLength];
		this.dataOffHeapMemory.get(itemOffsetInDataOffHeapMemory, result);
		return result;
	}
	

	@Override
	public int getKeyHash() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_KEY_HASH_CODE_OFFSET;
		int hashCode = this.indexOffHeapMemory.getInt(offsetInIndexOffHeapMemory);
		return hashCode;
	}

	@Override
	public long getTimeToLive() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_TIME_TO_LIVE_OFFSET;
		return this.indexOffHeapMemory.getLong(offsetInIndexOffHeapMemory);
	}

	@Override
	public long getCreatedTime() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_CREATED_TIME_OFFSET;
		return this.indexOffHeapMemory.getLong(offsetInIndexOffHeapMemory);
	}

	@Override
	public boolean isDeleted() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_STATUS;
		byte status = this.indexOffHeapMemory.getByte(offsetInIndexOffHeapMemory);
		return (status & ( 1 << 1)) != 0;
	}
	
	@Override
	public boolean isCompressed() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_STATUS;
		byte status = this.indexOffHeapMemory.getByte(offsetInIndexOffHeapMemory);
		return (status & ( 1 << 2)) != 0;
	}

	@Override
	public void markDeleted() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_STATUS;
		byte status = this.indexOffHeapMemory.getByte(offsetInIndexOffHeapMemory);
		status = (byte) (status | 1 << 1);
		this.indexOffHeapMemory.putByte(offsetInIndexOffHeapMemory, status);
	}

	@Override
	public boolean isInUse() {
		int offsetInIndexOffHeapMemory = AbstractMapTable.INDEX_ITEM_LENGTH * index + IMapEntry.INDEX_ITEM_STATUS;
		byte status = this.indexOffHeapMemory.getByte(offsetInIndexOffHeapMemory);
		return (status & 1) != 0;
	}

	@Override
	public boolean isExpired() {
		long ttl = this.getTimeToLive();
		if (ttl > 0) {
			if (System.currentTimeMillis() - this.getCreatedTime() > ttl) return true;
		}
		return false;
	}
}
