package com.ctriposs.lcache.table;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctriposs.lcache.offheap.OffHeapMemory;
import com.google.common.base.Preconditions;

public abstract class AbstractMapTable implements Closeable, Comparable<AbstractMapTable> {

	static final Logger log = LoggerFactory.getLogger(AbstractMapTable.class);

	static int SIZE_OF_LONG_IN_BYTES = 8;
	static int SIZE_OF_INT_IN_BYTES = 4;

	public final static int INIT_INDEX_ITEMS_PER_TABLE = 128 * 1024;
	// length in bytes of an index item
	final static int INDEX_ITEM_LENGTH = 40;
	// size in bytes of initial index offheap memory
    final static int INIT_INDEX_OFFHEAP_MEMORY_SIZE = INDEX_ITEM_LENGTH * INIT_INDEX_ITEMS_PER_TABLE;
	// size in bytes of initial data offheap memory
	public final static int INIT_DATA_OFFHEAP_MEMORY_SIZE = 128 * 1024 * 1024;

	public final static int NO_TIMEOUT = -1;

	protected boolean closed = false;

	protected AtomicInteger toAppendIndex;
	protected AtomicLong toAppendDataOffHeapMemoryOffset;
	protected final Lock appendLock = new ReentrantLock();

	// the level of the map store, start from 0, incremental.
	private int level;
	// the shard of the map store, start form 0, incremental.
	private short shard;
	// when this map store was created
	private long createdTime;
	
	protected OffHeapMemory indexOffHeapMemory;
	protected OffHeapMemory dataOffHeapMemory;

	public AbstractMapTable(short shard, int level, long createdTime) {
		this.shard = shard;
		this.level = level;
		this.createdTime = createdTime;
		
		this.toAppendIndex = new AtomicInteger(0);
		this.toAppendDataOffHeapMemoryOffset = new AtomicLong(0);
	}

	public IMapEntry getMapEntry(int index) {
		Preconditions.checkArgument(index >= 0, "index (%s) must be equal to or greater than 0", index);
		Preconditions.checkArgument(!isEmpty(), "Can't get map entry since the map is empty");
		return new OffHeapMapEntryImpl(index, this.indexOffHeapMemory, this.dataOffHeapMemory);
	}

	public int getAppendedItemCount() {
		return toAppendIndex.get();
	}

	public boolean isEmpty() {
		return toAppendIndex.get() == 0L;
	}

	@Override
	public int compareTo(AbstractMapTable mt) {
		if (level < mt.getLevel()) return -1;
		else if (level > mt.getLevel()) return 1;
		else {
			if (createdTime > mt.getCreatedTime()) return -1;
			else if (createdTime < mt.getCreatedTime()) return 1;
			else return 0;
		}
	}

	public int getLevel() {
		return level;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public short getShard() {
		return shard;
	}

	public abstract GetResult get(byte[] key) throws IOException;
	
	@Override
	public void close() {
		this.indexOffHeapMemory.free();
		this.dataOffHeapMemory.free();
		closed = true;
	}

	public int getUsedIndexOffHeapMemorySize() {
		return (int)this.indexOffHeapMemory.length();
	}

	public long getUsedDataOffHeapMemorySize() {
		return this.dataOffHeapMemory.length();
	}

	public long getTotalUsedOffHeapMemorySize() {
		return this.indexOffHeapMemory.length() + this.dataOffHeapMemory.length();
	}
}
