package com.ctriposs.lcache.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;

import com.ctriposs.lcache.LCache;
import com.ctriposs.lcache.LevelQueue;
import com.ctriposs.lcache.table.AbstractMapTable;
import com.ctriposs.lcache.table.ByteArrayWrapper;
import com.ctriposs.lcache.table.HashMapTable;
import com.ctriposs.lcache.table.IMapEntry;
import com.ctriposs.lcache.table.InMemIndex;
import com.ctriposs.lcache.table.OffHeapMapTable;
import com.ctriposs.lcache.utils.BytesUtil;
import com.ctriposs.lcache.utils.DateFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Level 0 to 1 merge sorting thread
 *
 * @author bulldog
 *
 */
public class Level0Merger extends Thread {

	static final Logger log = LoggerFactory.getLogger(Level0Merger.class);

	private static final int MAX_SLEEP_TIME = 2 * 1000; // 2 seconds
	public static final int DEFAULT_MERGE_WAYS = 2; // 2 way merge

	private List<LevelQueue> levelQueueList;
	
	private volatile boolean stop = false;
	private CountDownLatch countDownLatch;
	private short shard;

	public Level0Merger(List<LevelQueue> levelQueueList, CountDownLatch countDownLatch, short shard) {
		this.levelQueueList = levelQueueList;
		this.countDownLatch = countDownLatch;
		this.shard = shard;
	}

	@Override
	public void run() {
		while(!stop) {
			try {
				LevelQueue levelQueue0 = levelQueueList.get(LCache.LEVEL0);
				if (levelQueue0 != null && levelQueue0.size() >= DEFAULT_MERGE_WAYS) {
					log.info("Start running level 0 merge thread at " + DateFormatter.formatCurrentDate());
					log.info("Current queue size at level 0 is " + levelQueue0.size());

					LevelQueue levelQueue1 = levelQueueList.get(LCache.LEVEL1);
					mergeSort(levelQueue0, levelQueue1, DEFAULT_MERGE_WAYS, shard);

					log.info("Stopped running level 0 merge thread at " + DateFormatter.formatCurrentDate());

				} else {
					Thread.sleep(MAX_SLEEP_TIME);
				}

			} catch (Exception ex) {
				log.error("Error occured in the level0 merge dumper", ex);
			}

		}

		this.countDownLatch.countDown();
		log.info("Stopped level 0 merge thread " + this.getName());
	}

	public static void mergeSort(LevelQueue source, LevelQueue target, int ways, short shard) {
		List<HashMapTable> tables = new ArrayList<HashMapTable>(ways);
		source.getReadLock().lock();
		try {
			Iterator<AbstractMapTable> iter = source.descendingIterator();
			for(int i = 0; i < ways; i++) {
				tables.add((HashMapTable) iter.next());
			}
		} finally {
			source.getReadLock().unlock();
		}

		int expectedInsertions = 0;
		long expectedDataSize = 0;
		for(HashMapTable table : tables) {
			expectedInsertions += table.getRealItemCount();
			expectedDataSize += table.getUsedDataOffHeapMemorySize();
		}
		// target table
		OffHeapMapTable sortedMapTable = new OffHeapMapTable(shard, LCache.LEVEL1, System.nanoTime(), expectedInsertions, expectedDataSize);

		PriorityQueue<QueueElement> pq = new PriorityQueue<QueueElement>();
		// build initial heap
		for(HashMapTable table : tables) {
			QueueElement qe = new QueueElement();
			final HashMapTable hmTable = table;
			qe.hashMapTable = hmTable;
			List<Map.Entry<ByteArrayWrapper, InMemIndex>> list = new ArrayList<Map.Entry<ByteArrayWrapper, InMemIndex>>(qe.hashMapTable.getEntrySet());
			Collections.sort(list, new Comparator<Map.Entry<ByteArrayWrapper, InMemIndex>>() {

				@Override
				public int compare(
						Entry<ByteArrayWrapper, InMemIndex> o1,
						Entry<ByteArrayWrapper, InMemIndex> o2) {
					IMapEntry mapEntry1 = hmTable.getMapEntry(o1.getValue().getIndex());
					IMapEntry mapEntry2 = hmTable.getMapEntry(o2.getValue().getIndex());
					
					int hash1 = mapEntry1.getKeyHash();
					int hash2 = mapEntry2.getKeyHash();
					if (hash1 < hash2) return -1;
					else if (hash1 > hash2) return 1;
					else {
						return o1.getKey().compareTo(o2.getKey());
				    }

				}

			});
			qe.iterator = list.iterator();
			if (qe.iterator.hasNext()) {
				Map.Entry<ByteArrayWrapper, InMemIndex> me = qe.iterator.next();
				qe.key = me.getKey().getData();
				qe.inMemIndex = me.getValue();
				IMapEntry mapEntry = table.getMapEntry(qe.inMemIndex.getIndex());
				qe.keyHash = mapEntry.getKeyHash();
				pq.add(qe);
			}
		}

		// merge sort
		while(pq.size() > 0) {
			QueueElement qe1 = pq.poll();
			// remove old/stale entries
			while(pq.peek() != null && qe1.keyHash == pq.peek().keyHash && BytesUtil.compare(qe1.key, pq.peek().key) == 0) {
				QueueElement qe2 = pq.poll();
				if (qe2.iterator.hasNext()) {
					Map.Entry<ByteArrayWrapper, InMemIndex> me = qe2.iterator.next();
					qe2.key = me.getKey().getData();
					qe2.inMemIndex = me.getValue();
					IMapEntry mapEntry = qe2.hashMapTable.getMapEntry(qe2.inMemIndex.getIndex());
					qe2.keyHash = mapEntry.getKeyHash();
					pq.add(qe2);
				}
			}

			IMapEntry mapEntry = qe1.hashMapTable.getMapEntry(qe1.inMemIndex.getIndex());
			byte[] value = mapEntry.getValue();
			// disk space optimization
			if (mapEntry.isDeleted() || mapEntry.isExpired()) {
				value = new byte[] {0};
			}
			sortedMapTable.appendNew(mapEntry.getKey(), mapEntry.getKeyHash(), value, mapEntry.getTimeToLive(), mapEntry.getCreatedTime(), mapEntry.isDeleted(), mapEntry.isCompressed());

			if (qe1.iterator.hasNext()) {
				Map.Entry<ByteArrayWrapper, InMemIndex> me = qe1.iterator.next();
				qe1.key = me.getKey().getData();
				qe1.inMemIndex = me.getValue();
				IMapEntry mEntry = qe1.hashMapTable.getMapEntry(qe1.inMemIndex.getIndex());
				qe1.keyHash = mEntry.getKeyHash();
				pq.add(qe1);
			}
		}

		// save memory
		sortedMapTable.truncate();

		// dump to level 1
		source.getWriteLock().lock();
		target.getWriteLock().lock();
		try {
			for(int i = 0; i < ways; i++) {
				source.removeLast();
			}

			target.addFirst(sortedMapTable);

		} finally {
			target.getWriteLock().unlock();
			source.getWriteLock().unlock();
		}

		for(HashMapTable table : tables) {
			table.close();
		}
	}

	public void setStop() {
		this.stop = true;
		log.info("Stopping level 0 merge thread " + this.getName());
	}

	static class QueueElement implements Comparable<QueueElement> {
		HashMapTable hashMapTable;
		Iterator<Map.Entry<ByteArrayWrapper, InMemIndex>> iterator;
		int keyHash;
		byte[] key;
		InMemIndex inMemIndex;

		@Override
		public int compareTo(QueueElement other) {
			if (keyHash < other.keyHash) return -1;
			else if (keyHash > other.keyHash) return 1;
			else {
				if (BytesUtil.compare(key, other.key) < 0) {
					return -1;
				} else if (BytesUtil.compare(key, other.key) > 0) {
					return 1;
				} else {
					if (hashMapTable.getCreatedTime() > other.hashMapTable.getCreatedTime()) {
						return -1;
					} else if (hashMapTable.getCreatedTime() < other.hashMapTable.getCreatedTime()) {
						return 1;
					} else {
						return 0;
					}
				}
			}
		}

	}

}
