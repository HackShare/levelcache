package com.ctriposs.lcache;

import com.ctriposs.lcache.stats.AvgStats;
import com.ctriposs.lcache.stats.LCacheStats;
import com.ctriposs.lcache.stats.SingleStats;
import com.ctriposs.lcache.utils.TestUtil;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static com.ctriposs.lcache.stats.LCacheStats.*;

public class LCacheTest {

	// You can set the STRESS_FACTOR system property to make the tests run more iterations.
	public static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTOR", "1.3"));

	private LCache cache;

	@Test
	public void testDB() {
		cache = new LCache();

		Set<String> rndStringSet = new HashSet<String>();
		for (int i = 0; i < 2000000 * STRESS_FACTOR; i++) {
			String rndString = TestUtil.randomString(64);
			rndStringSet.add(rndString);
			cache.put(rndString.getBytes(), rndString.getBytes());
			if ((i % 50000) == 0 && i != 0) {
				System.out.println(i + " rows written");
			}
		}

		for (String rndString : rndStringSet) {
			byte[] value = cache.get(rndString.getBytes());
			assertNotNull(value);
			assertEquals(rndString, new String(value));
		}

		// delete
		for (String rndString : rndStringSet) {
			cache.delete(rndString.getBytes());
		}

		for (String rndString : rndStringSet) {
			byte[] value = cache.get(rndString.getBytes());
			assertNull(value);
		}

		LCacheStats stats = cache.getStats();
		long inMemPut = getAvgStatsCount(stats, PUT_INMEM);
		assertEquals(rndStringSet.size(), inMemPut);
		long inMemGet = getAvgStatsCount(stats, GET_INMEM),
				level0Get = getAvgStatsCount(stats, GET_LEVEL0),
				level1Get = getAvgStatsCount(stats, GET_LEVEL1),
				level2Get = getAvgStatsCount(stats, GET_LEVEL2);
		assertEquals(rndStringSet.size() * 2, inMemGet + level0Get + level1Get + level2Get);

		// Make sure MemStatsCollector has enough time to finish up its work.
		try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
		}

		long totalMemSize =getSingleStatsValue(stats, MEM_SIZE_TOTAL);
		long activeInMemSize = getSingleStatsValue(stats, MEM_SIZE_INMEM),
				level0MemSize = getSingleStatsValue(stats, MEM_SIZE_LEVEL0),
				level1MemSize = getSingleStatsValue(stats, MEM_SIZE_LEVEL1),
				level2MemSize = getSingleStatsValue(stats, MEM_SIZE_LEVEL2);
		assertEquals(totalMemSize, activeInMemSize + level0MemSize + level1MemSize + level2MemSize);

		outputStats(stats);
	}

	@After
	public void clear() throws IOException {
		if (cache != null) {
			cache.close();
		}
	}

	private static long getAvgStatsCount(LCacheStats stats, String name) {
		AtomicReference<AvgStats> ref = stats.getAvgStatsMap().get(name);
		return ref != null ? ref.get().getCount() : 0;
	}

	private static long getSingleStatsValue(LCacheStats stats, String name) {
		AtomicReference<SingleStats> ref = stats.getSingleStatsMap().get(name);
		return ref != null ? ref.get().getValue() : 0;
	}

	public static void outputStats(LCacheStats stats) {
		for (String key : stats.getAvgStatsMap().keySet()) {
			AvgStats avgStats = stats.getAvgStatsMap().get(key).get();
			if (avgStats.getCount() == 0) {
				continue;
			}
			System.out.printf("%s: Count %d    Min %d    Max %d   Avg %d", key, avgStats.getCount(), avgStats.getMin(),
					avgStats.getMax(), avgStats.getAvg());
			System.out.println();
		}

		for (String key : stats.getSingleStatsMap().keySet()) {
			SingleStats singleStats = stats.getSingleStatsMap().get(key).get();
			if (singleStats.getValue() == 0) {
				continue;
			}
			System.out.printf("%s: %d", key, singleStats.getValue());
			System.out.println();
		}
	}
}
