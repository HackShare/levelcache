package com.ctriposs.lcache;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.ctriposs.lcache.utils.BytesUtil;
import com.ctriposs.lcache.utils.DateFormatter;
import com.ctriposs.lcache.utils.TestUtil;

public class StressTest {

	public static void main(String[] args) {
		
		int keyLimit = 1024 * 16;
		int valueLengthLimit = 1024 * 16;
		
		LCache cache = new LCache();
		
		Map<String, byte[]> map = new HashMap<String, byte[]>();
		
		String rndString = TestUtil.randomString(valueLengthLimit);
		byte[] rndBytes = rndString.getBytes();
		
		Random random = new Random();
		
		System.out.println("Start from date " + DateFormatter.formatCurrentDate());
		long start = System.currentTimeMillis();
		for(long counter = 0;; counter++) {
			int rndKey = random.nextInt(keyLimit);
			boolean put = random.nextDouble() < 0.5 ? true : false;
			if (put) {
				map.put(String.valueOf(rndKey), rndBytes);
				cache.put(String.valueOf(rndKey).getBytes(), rndBytes);
			} else {
				map.remove(String.valueOf(rndKey));
				cache.delete(String.valueOf(rndKey).getBytes());
			}
			if (counter%1000000 == 0) {
				System.out.println("Current date " + DateFormatter.formatCurrentDate());
				System.out.println(""+counter);
				System.out.println(TestUtil.printMemoryFootprint());
				long end = System.currentTimeMillis();
				System.out.println("timeSpent = " + (end - start));
				start = System.currentTimeMillis();
				
				// stats
				LCacheTest.outputStats(cache.getStats());
				
				// validation
				for(int i = 0; i < keyLimit; i++) {
					String key = String.valueOf(i);
					byte[] mapValue = map.get(key);
					byte[] cacheValue = cache.get(key.getBytes());
					if (mapValue == null && cacheValue != null) {
						throw new RuntimeException("Validation exception, key exists in cache but not in map");
					}
					if (mapValue != null && cacheValue == null) {
						throw new RuntimeException("Validation exception, key exists in map but not in cache");
					}
					if (mapValue != null && cacheValue != null) {
						if (BytesUtil.compare(mapValue, cacheValue) != 0) {
							throw new RuntimeException("Validation exception, values in map and cache do not equal");
						}
					}
				}
				
			}
			
		}

	}

}
