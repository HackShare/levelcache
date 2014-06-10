package com.ctriposs.lcache.table;

import java.io.IOException;
import java.util.Arrays;

import org.xerial.snappy.Snappy;

import com.ctriposs.lcache.offheap.OffHeapMemory;
import com.ctriposs.lcache.utils.BytesUtil;
import com.google.common.base.Preconditions;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public abstract class AbstractSortedMapTable extends AbstractMapTable {

	public static final String BLOOM_FLITER_FILE_SUFFIX = ".bloom";
	public static final float FALSE_POSITIVE_PROBABILITY = 0.001F;
	public static final int MAX_ALLOWED_NUMBER_OF_ENTRIES = Integer.MAX_VALUE / INDEX_ITEM_LENGTH;

	protected BloomFilter<byte[]> bloomFilter;

	public AbstractSortedMapTable(short shard, int level, long createdTime, int expectedInsertions, long expectedDataOffHeapMemorySize) {
		super(shard, level, createdTime);
		this.createNewBloomFilter(expectedInsertions);
		
		int indexOffHeapMemorySize = INDEX_ITEM_LENGTH * expectedInsertions;
		this.indexOffHeapMemory = OffHeapMemory.allocateMemory(indexOffHeapMemorySize);
		
		this.dataOffHeapMemory = OffHeapMemory.allocateMemory(expectedDataOffHeapMemorySize);
	}


	private void createNewBloomFilter(int expectedInsertions) {
		bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), expectedInsertions, FALSE_POSITIVE_PROBABILITY);
	}

	// Search the key in the hashcode sorted array
	private IMapEntry binarySearch(byte[] key) {
		int hashCode = Arrays.hashCode(key);
		int lo = 0; int slo = lo;
		int hi = this.getAppendedItemCount() - 1; int shi = hi;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			IMapEntry mapEntry = this.getMapEntry(mid);
			int midHashCode = mapEntry.getKeyHash();
			if (hashCode < midHashCode) hi = mid - 1;
			else if (hashCode > midHashCode) lo = mid + 1;
			else {
				if (BytesUtil.compare(key, mapEntry.getKey()) == 0) {
					return mapEntry;
				}
				// find left
				int index = mid - 1;
				while(index >= slo) {
					mapEntry = this.getMapEntry(index);
					if (hashCode != mapEntry.getKeyHash()) break;
					if (BytesUtil.compare(key, mapEntry.getKey()) == 0) {
						return mapEntry;
					}
					index--;
				}
				// find right
				index = mid + 1;
				while(index <= shi) {
					mapEntry = this.getMapEntry(index);
					if (hashCode != mapEntry.getKeyHash()) break;
					if (BytesUtil.compare(key, mapEntry.getKey()) == 0) {
						return mapEntry;
					}
					index++;
				}

				return null;
			}
		}
		return null;
	}

	@Override
	public GetResult get(byte[] key) throws IOException {
		Preconditions.checkArgument(key != null && key.length > 0, "Key is empty");
		Preconditions.checkArgument(this.getAppendedItemCount() >= 1, "the map table is empty");
		GetResult result = new GetResult();

		// leverage bloom filter for guarded condition
		if (!this.bloomFilter.mightContain(key)) return result;

		IMapEntry mapEntry = this.binarySearch(key);
		if (mapEntry == null) return result;
		else {
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
			// hint for locality
			result.setLevel(this.getLevel());
			result.setTimeToLive(mapEntry.getTimeToLive());
			result.setCreatedTime(mapEntry.getCreatedTime());

			return result;
		}
	}


	// memory saving
	public void truncate() {
		long realIndexSize = INDEX_ITEM_LENGTH * toAppendIndex.get();
		if (realIndexSize > 0 && realIndexSize < this.indexOffHeapMemory.length()) {
			OffHeapMemory tempOffHeampMemory = OffHeapMemory.allocateMemory(realIndexSize);
			this.indexOffHeapMemory.copy(0, tempOffHeampMemory, 0, realIndexSize);
			this.indexOffHeapMemory.free();
			this.indexOffHeapMemory = tempOffHeampMemory;
		}
		
		long realDataSize = this.toAppendDataOffHeapMemoryOffset.get();
		if (realDataSize > 0 && realDataSize < this.dataOffHeapMemory.length()) {
			OffHeapMemory tempOffHeampMemory = OffHeapMemory.allocateMemory(realDataSize);
			this.dataOffHeapMemory.copy(0, tempOffHeampMemory, 0, realDataSize);
			this.dataOffHeapMemory.free();
			this.dataOffHeapMemory = tempOffHeampMemory;
		}
	}

	public abstract IMapEntry appendNew(byte[] key, int keyHash, byte[] value, long timeToLive, long lastAccessedTime, boolean markDelete, boolean compressed);

}
