package com.ctriposs.lcache.stats;

import com.ctriposs.lcache.LCache;
import com.ctriposs.lcache.LevelQueue;
import com.ctriposs.lcache.table.AbstractMapTable;
import com.ctriposs.lcache.table.HashMapTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author yqdong
 */
public class MemStatsCollector extends Thread {

    private static final Logger log = LoggerFactory.getLogger(MemStatsCollector.class);
    private static final int MAX_SLEEP_TIME = 5 * 1000; // 5 seconds

    private final LCacheStats stats;
    private final HashMapTable[] activeInMemTables;
    private final List<LevelQueue>[] levelQueueLists;
    private volatile Boolean stop = false;

    public MemStatsCollector(LCacheStats stats, HashMapTable[] activeInMemTables, List<LevelQueue>[] levelQueueLists) {
        this.stats = stats;
        this.activeInMemTables = activeInMemTables;
        this.levelQueueLists = levelQueueLists;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
                long totalMemSize = 0;

                long activeInMemSize = 0;
                for (int shard = 0; shard < activeInMemTables.length; ++shard) {
                    activeInMemSize += activeInMemTables[shard].getTotalUsedOffHeapMemorySize();
                }
                totalMemSize += activeInMemSize;
                stats.recordMemStats(LCache.INMEM_LEVEL, activeInMemSize);

                for (int level = 0; level <= LCache.MAX_LEVEL; ++level) {
                    long memSize = 0;
                    for (int shard = 0; shard < levelQueueLists.length; ++shard) {
                        LevelQueue queue = levelQueueLists[shard].get(level);
                        queue.getReadLock().lock();
                        try {
                            for (AbstractMapTable table : queue) {
                                memSize += table.getTotalUsedOffHeapMemorySize();
                            }
                        } finally {
                            queue.getReadLock().unlock();
                        }
                    }
                    totalMemSize += memSize;
                    stats.recordMemStats(level, memSize);
                }
                stats.recordTotalMemStats(totalMemSize);
                Thread.sleep(MAX_SLEEP_TIME);
            } catch (Exception ex) {
                log.error("Error occurred in the mem stats collector", ex);
            }
        }
    }

    public void setStop() {
        this.stop = true;
        log.info("Stopping mem stats collector thread " + this.getName());
    }
}
