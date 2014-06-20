package com.ctriposs.lcache.stats;

import com.ctriposs.lcache.LCache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yqdong
 */
public class LCacheStats {

    public static final String GET_INMEM = Operations.GET + ".inMem.cost";
    public static final String GET_LEVEL0 = Operations.GET + ".level0.cost";
    public static final String GET_LEVEL1 = Operations.GET + ".level1.cost";
    public static final String GET_LEVEL2 = Operations.GET + ".level2.cost";
    public static final String PUT_INMEM = Operations.PUT + ".inMem.cost";
    public static final String DELETE_INMEM = Operations.DELETE + ".inMem.cost";

    public static final String MERGING_LEVEL0= "merging.level0.cost";
    public static final String MERGING_LEVEL1 = "merging.level1.cost";

    public static final String MEM_SIZE_TOTAL = "storage.memSize";
    public static final String MEM_SIZE_INMEM = "storage.inMem.memSize";
    public static final String MEM_SIZE_LEVEL0 = "storage.level0.memSize";
    public static final String MEM_SIZE_LEVEL1 = "storage.level1.memSize";
    public static final String MEM_SIZE_LEVEL2 = "storage.level2.memSize";

    private final ConcurrentHashMap<String, AtomicReference<AvgStats>> avgStatsMap = new ConcurrentHashMap<String, AtomicReference<AvgStats>>();

    private final ConcurrentHashMap<String, AtomicReference<SingleStats>> singleStatsMap = new ConcurrentHashMap<String, AtomicReference<SingleStats>>();

    public LCacheStats() {
        initStats();
    }

    public ConcurrentHashMap<String, AtomicReference<AvgStats>> getAvgStatsMap() {
        return avgStatsMap;
    }

    public ConcurrentHashMap<String, AtomicReference<SingleStats>> getSingleStatsMap() {
        return singleStatsMap;
    }

    public void recordOperation(String operation, int reachedLevel, long cost) {
        String prefix = operation + (reachedLevel != LCache.INMEM_LEVEL ?  ".level" + reachedLevel : ".inMem");

        AtomicReference<AvgStats> avgRef = getAvgStats(prefix + ".cost");
        avgRef.get().addValue(cost);
    }

    public void recordError(String operation) {
        AtomicReference<SingleStats> ref = getSingleStats(operation + ".error");
        ref.get().increaseValue();
    }

    public void recordMerging(int level, long cost) {
        String prefix = "merging.level" + level;

        AtomicReference<AvgStats> avgRef = getAvgStats(prefix + ".cost");
        avgRef.get().addValue(cost);
    }

    public void recordTotalMemStats(long memSize) {
        AtomicReference<SingleStats>  singleRef = getSingleStats(MEM_SIZE_TOTAL);
        singleRef.get().setValue(memSize);
    }

    public void recordMemStats(int level, long memSize) {
        String prefix = "storage" + (level != LCache.INMEM_LEVEL ?  ".level" + level : ".inMem");

        AtomicReference<SingleStats>  singleRef = getSingleStats(prefix + ".memSize");
        singleRef.get().setValue(memSize);
    }

    private AtomicReference<AvgStats> getAvgStats(String key) {
        AtomicReference<AvgStats> ref = avgStatsMap.get(key);
        if (ref == null) {
            ref = new AtomicReference<AvgStats>(new AvgStats());
            AtomicReference<AvgStats> found = avgStatsMap.putIfAbsent(key, ref);
            if (found != null) {
                ref = found;
            }
        }
        return ref;
    }

    private AtomicReference<SingleStats> getSingleStats(String key) {
        AtomicReference<SingleStats> ref = singleStatsMap.get(key);
        if (ref == null) {
            ref = new AtomicReference<SingleStats>(new SingleStats());
            AtomicReference<SingleStats> found = singleStatsMap.putIfAbsent(key, ref);
            if (found != null) {
                ref = found;
            }
        }
        return ref;
    }

    private void initStats() {
        getAvgStats(GET_INMEM);
        getAvgStats(GET_LEVEL0);
        getAvgStats(GET_LEVEL1);
        getAvgStats(GET_LEVEL2);

        getAvgStats(PUT_INMEM);
        getAvgStats(DELETE_INMEM);

        getAvgStats(MERGING_LEVEL0);
        getAvgStats(MERGING_LEVEL1);

        getSingleStats(MEM_SIZE_TOTAL);
        getSingleStats(MEM_SIZE_INMEM);
        getSingleStats(MEM_SIZE_LEVEL0);
        getSingleStats(MEM_SIZE_LEVEL1);
        getSingleStats(MEM_SIZE_LEVEL2);
    }
}
