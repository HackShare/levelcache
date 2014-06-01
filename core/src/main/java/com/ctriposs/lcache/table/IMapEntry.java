package com.ctriposs.lcache.table;

public interface IMapEntry {
	
	final static int INDEX_ITEM_IN_DATA_OFFHEAP_MEMORY_OFFSET_OFFSET = 0;
	final static int INDEX_ITEM_KEY_LENGTH_OFFSET = 8;
	final static int INDEX_ITEM_VALUE_LENGTH_OFFSET = 12;
	final static int INDEX_ITEM_TIME_TO_LIVE_OFFSET = 16;
	final static int INDEX_ITEM_CREATED_TIME_OFFSET = 24;
	final static int INDEX_ITEM_KEY_HASH_CODE_OFFSET = 32;
	final static int INDEX_ITEM_STATUS = 36;
	
	int getIndex();
	
    byte[] getKey();
	
	byte[] getValue();
	
	int getKeyHash();
	
	long getTimeToLive();
	
	long getCreatedTime();

	boolean isDeleted();
	
	void markDeleted();
	
	boolean isInUse();
	
	boolean isExpired();
	
	boolean isCompressed();
}
