package com.ctriposs.lcache;

public class CacheConfig {

	public static final CacheConfig SMALL = new CacheConfig().setShardNumber((short)1);
	public static final CacheConfig DEFAULT = new CacheConfig().setShardNumber((short)4);
	public static final CacheConfig BIG = new CacheConfig().setShardNumber((short)8);
	public static final CacheConfig LARGE = new CacheConfig().setShardNumber((short)16);
	public static final CacheConfig HUGE = new CacheConfig().setShardNumber((short)32);

	private short shardNumber = 4;

	private boolean compressionEnabled = true;
	private boolean localityEnabled = false;

	private long maxMemCapacity = 0;

	public boolean isCompressionEnabled() {
		return compressionEnabled;
	}

	public boolean isLocalityEnabled() {
		return this.localityEnabled;
	}

	/**
	 * Important: shard number can't be changed for an existing DB.
	 *
	 * @return shard number
	 */
	public short getShardNumber() {
		return shardNumber;
	}

	/**
	 * Enable snappy compression for value
	 *
	 * @param compressionEnabled
	 * @return Session DB configuration
	 */
	public CacheConfig setCompressionEnabled(boolean compressionEnabled) {
		this.compressionEnabled = compressionEnabled;
		return this;
	}

	private CacheConfig setShardNumber(short shardNumber) {
		this.shardNumber = shardNumber;
		return this;
	}

	/**
	 * Enable data access locality, if enabled, when a key/value entry is found in Level 2 FCMapTable,
	 * it will be moved to current active HashMapTable for locality.
	 *
	 * @param localityEnabled
	 * @return Session DB configuration
	 */
	public CacheConfig setLocalityEnabled(boolean localityEnabled) {
		this.localityEnabled = localityEnabled;
		return this;
	}

	/**
	 * Returns the max heap memory capacity the LevelCache can use.
	 *
	 * A zero or negative value means there is no limitation.
	 * The default value is 0.
	 *
	 * @return the max heap memory capacity the LevelCache can use
	 */
	public long getMaxMemCapacity() {
		return maxMemCapacity;
	}

	/**
	 * Sets the max heap memory capacity the LevelCache can use.
	 *
	 * A zero or negative value means there is no limitation.
	 *
	 * @param maxMemCapacity is the max heap memory capacity the LevelCache can use.
	 */
	public void setMaxMemCapacity(long maxMemCapacity) {
		this.maxMemCapacity = maxMemCapacity;
	}
}
