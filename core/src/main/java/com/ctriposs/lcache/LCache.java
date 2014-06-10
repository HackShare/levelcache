package com.ctriposs.lcache;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.ctriposs.lcache.merge.Level0Merger;
import com.ctriposs.lcache.merge.Level1Merger;
import com.ctriposs.lcache.stats.LCacheStats;
import com.ctriposs.lcache.stats.MemStatsCollector;
import com.ctriposs.lcache.stats.Operations;
import com.ctriposs.lcache.table.AbstractMapTable;
import com.ctriposs.lcache.table.GetResult;
import com.ctriposs.lcache.table.HashMapTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A Big, Fast, In-Memory K/V Cache, Tailored for Session Data
 *
 * @author bulldog
 *
 */
public class LCache implements Closeable {

	static final Logger log = LoggerFactory.getLogger(LCache.class);

	public static final int INMEM_LEVEL = -1;
	public static final int LEVEL0 = 0;
	public static final int LEVEL1 = 1;
	public static final int LEVEL2 = 2;
	public static final int MAX_LEVEL = 2;
	private volatile HashMapTable[] activeInMemTables;
	private Object[] activeInMemTableCreationLocks;
	private List<LevelQueue>[] levelQueueLists;
	private final LCacheStats stats = new LCacheStats();

	private CacheConfig config;
	private Level0Merger[] level0Mergers;
	private Level1Merger[] level1Mergers;
	private CountDownLatch[] countDownLatches;
	private MemStatsCollector memStatsCollector;

	private boolean closed = false;

	public LCache() {
		this(CacheConfig.DEFAULT);
	}

	@SuppressWarnings("unchecked")
	public LCache(CacheConfig config) {
		this.config = config;

		activeInMemTables = new HashMapTable[config.getShardNumber()];

		activeInMemTableCreationLocks = new Object[config.getShardNumber()];
		for(int i = 0; i < config.getShardNumber(); i++) {
			activeInMemTableCreationLocks[i] = new Object();
		}

		// initialize level queue list
		levelQueueLists = new ArrayList[config.getShardNumber()];
		for(int i = 0; i < config.getShardNumber(); i++) {
			levelQueueLists[i] = new ArrayList<LevelQueue>(MAX_LEVEL + 1);
			for(int j = 0; j <= MAX_LEVEL; j++) {
				levelQueueLists[i].add(new LevelQueue());
			}
		}

		for(short i = 0; i < this.config.getShardNumber(); i++) {
			this.activeInMemTables[i] = new HashMapTable(i, LEVEL0, System.nanoTime());
			this.activeInMemTables[i].markImmutable(false); // mutable
			this.activeInMemTables[i].setCompressionEnabled(this.config.isCompressionEnabled());
		}

		memStatsCollector = new MemStatsCollector(stats, levelQueueLists);
		memStatsCollector.start();

		this.startLevelMergers();
	}

	private void startLevelMergers() {
		countDownLatches = new CountDownLatch[this.config.getShardNumber()];
		for(int i = 0; i < this.config.getShardNumber(); i++) {
			countDownLatches[i] = new CountDownLatch(2);
		}
		level0Mergers = new Level0Merger[config.getShardNumber()];
		level1Mergers = new Level1Merger[config.getShardNumber()];

		for(short i = 0; i < this.config.getShardNumber(); i++) {
			level0Mergers[i] = new Level0Merger(this.levelQueueLists[i], countDownLatches[i], i, stats);
			level0Mergers[i].start();
			level1Mergers[i] = new Level1Merger(this.levelQueueLists[i], countDownLatches[i], i, stats);
			level1Mergers[i].start();
		}
	}

	public CacheConfig getConfig() { return this.config; }

	public LCacheStats getStats() {
		return this.stats;
	}

	/**
	 * Put key/value entry into the DB with no timeout
	 *
	 * @param key the map entry key
	 * @param value the map entry value
	 */
	public void put(byte[] key, byte[] value) {
		this.put(key, value, AbstractMapTable.NO_TIMEOUT, System.currentTimeMillis(), false);
	}

	/**
	 * Put key/value entry into the DB with specific timeToLive
	 *
	 * @param key the map entry key
	 * @param value the map entry value
	 * @param timeToLive time to live
	 */
	public void put(byte[] key, byte[] value, long timeToLive) {
		this.put(key, value, timeToLive, System.currentTimeMillis(), false);
	}

	/**
	 * Delete map entry in the DB with specific key
	 *
	 * @param key the map entry key
	 */
	public void delete(byte[] key) {
		this.put(key, new byte[] {0}, AbstractMapTable.NO_TIMEOUT, System.currentTimeMillis(), true);
	}

	private short getShard(byte[] key) {
		int keyHash = Arrays.hashCode(key);
		keyHash = Math.abs(keyHash);
		return (short) (keyHash % this.config.getShardNumber());
	}

	private void put(byte[] key, byte[] value, long timeToLive, long createdTime, boolean isDelete) {
		Preconditions.checkArgument(key != null && key.length > 0, "key is empty");
		Preconditions.checkArgument(value != null && value.length > 0, "value is empty");
		ensureNotClosed();
		long start = System.nanoTime();
		String operation = isDelete ? Operations.DELETE : Operations.PUT;
		try {
			short shard = this.getShard(key);
			boolean success = this.activeInMemTables[shard].put(key, value, timeToLive, createdTime, isDelete);

			if (!success) { // overflow
				synchronized(activeInMemTableCreationLocks[shard]) {
					success = this.activeInMemTables[shard].put(key, value, timeToLive, createdTime, isDelete); // other thread may have done the creation work
					if (!success) { // move to level queue 0
						this.activeInMemTables[shard].markImmutable(true);
						LevelQueue lq0 = this.levelQueueLists[shard].get(LEVEL0);
						lq0.getWriteLock().lock();
						try {
							lq0.addFirst(this.activeInMemTables[shard]);
						} finally {
							lq0.getWriteLock().unlock();
						}

						@SuppressWarnings("resource")
						HashMapTable tempTable = new HashMapTable(shard, LEVEL0, System.nanoTime());
						tempTable.markImmutable(false); //mutable
						tempTable.put(key, value, timeToLive, createdTime, isDelete);
						// switch on
						this.activeInMemTables[shard] = tempTable;
					}
				}
			}
		} catch(IOException ioe) {
			stats.recordError(operation);
			if (isDelete) {
				throw new RuntimeException("Fail to delete key, IOException occurr", ioe);
			}
			throw new RuntimeException("Fail to put key & value, IOException occurr", ioe);
		} finally {
			stats.recordOperation(operation, INMEM_LEVEL, System.nanoTime() - start);
		}
	}

	/**
	 * Get value in the DB with specific key
	 *
	 * @param key map entry key
	 * @return non-null value if the entry exists, not deleted or expired.
	 * null value if the entry does not exist, or exists but deleted or expired.
	 */
	public byte[] get(byte[] key) {
		Preconditions.checkArgument(key != null && key.length > 0, "key is empty");
		ensureNotClosed();
		long start = System.nanoTime();
		int reachedLevel = INMEM_LEVEL;
		try {
			short shard = this.getShard(key);
			// check active hashmap table first
			GetResult result = this.activeInMemTables[shard].get(key);
			if (result.isFound()) {
				if (!result.isDeleted() && !result.isExpired()) {
					return result.getValue();
				} else {
					return null; // deleted or expired
				}
			} else {
				// check level0 hashmap tables
				reachedLevel = LEVEL0;
				LevelQueue lq0 = levelQueueLists[shard].get(LEVEL0);
				lq0.getReadLock().lock();
				try {
					if (lq0 != null && lq0.size() > 0) {
						for(AbstractMapTable table : lq0) {
							result = table.get(key);
							if (result.isFound()) break;
						}
					}
				} finally {
					lq0.getReadLock().unlock();
				}

				if (result.isFound()) {
					if (!result.isDeleted() && !result.isExpired()) {
						if (result.getLevel() == LCache.LEVEL2 && this.config.isLocalityEnabled()) { // keep locality
							this.put(key, result.getValue(), result.getTimeToLive(), result.getCreatedTime(), false);
						}
						return result.getValue();
					} else {
						return null; // deleted or expired
					}
				}

				// check level 1-2 offheap sorted tables
				searchLevel12: {
					for(int level = 1; level <= MAX_LEVEL; level++) {
						reachedLevel = level;
						LevelQueue lq = levelQueueLists[shard].get(level);
						lq.getReadLock().lock();
						try {
							if (lq.size() > 0) {
								for(AbstractMapTable table : lq) {
									result = table.get(key);
									if (result.isFound()) break searchLevel12;
								}
							}
						} finally {
							lq.getReadLock().unlock();
						}
					}
				}

				if (result.isFound()) {
					if (!result.isDeleted() && !result.isExpired()) {
						if (result.getLevel() == LCache.LEVEL2 && this.config.isLocalityEnabled()) { // keep locality
							this.put(key, result.getValue(), result.getTimeToLive(), result.getCreatedTime(), false);
						}
						return result.getValue();
					} else {
						return null; // deleted or expired
					}
				}
			}
		}
		catch(IOException ioe) {
			stats.recordError(Operations.GET);
			throw new RuntimeException("Fail to get value by key, IOException occurr", ioe);
		} finally {
			stats.recordOperation(Operations.GET, reachedLevel, System.nanoTime() - start);
		}

		return null; // no luck
	}

	@Override
	public void close() throws IOException {
		if (closed) return;

		memStatsCollector.setStop();

		for(int i = 0; i < config.getShardNumber(); i++) {
			this.activeInMemTables[i].close();
		}

		for(int i = 0; i < config.getShardNumber(); i++) {
			this.level0Mergers[i].setStop();
			this.level1Mergers[i].setStop();
		}

		for(int i = 0; i < config.getShardNumber(); i++) {
			try {
				log.info("Shard " + i + " waiting level 0 & 1 merge threads to exit...");
				this.countDownLatches[i].await();
			} catch (InterruptedException e) {
				// ignore;
			}

		}

		for(int i = 0; i < config.getShardNumber(); i++) {
			for(int j = 0; j <= MAX_LEVEL; j++) {
				LevelQueue lq = this.levelQueueLists[i].get(j);
				for(AbstractMapTable table : lq) {
					table.close();
				}

			}
		}

		closed = true;
		log.info("Cache Closed.");
	}

	protected void ensureNotClosed() {
		if (closed) {
			throw new IllegalStateException("You can't work on a closed LevelCache.");
		}
	}
}
