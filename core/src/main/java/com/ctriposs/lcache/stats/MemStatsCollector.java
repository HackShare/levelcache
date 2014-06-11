package com.ctriposs.lcache.stats;

import com.ctriposs.lcache.LCache;
import com.ctriposs.lcache.LevelQueue;
import com.ctriposs.lcache.table.AbstractMapTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author yqdong
 */
public class MemStatsCollector extends Thread {

    private static final Logger log = LoggerFactory.getLogger(MemStatsCollector.class);
    private static final int MAX_SLEEP_TIME = 10 * 1000; // 10 second

    private final LCacheStats stats;
    private final List<LevelQueue>[] levelQueueLists;
    private volatile Boolean stop = false;

    public MemStatsCollector(LCacheStats stats, List<LevelQueue>[] levelQueueLists) {
        this.stats = stats;
        this.levelQueueLists = levelQueueLists;
    }

    @Override
    public void run() {
        while (!stop) {
            try {
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
                    stats.recordMemStats(level, memSize);
                }

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
