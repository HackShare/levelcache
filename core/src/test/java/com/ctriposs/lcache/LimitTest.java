package com.ctriposs.lcache;

import com.ctriposs.lcache.CacheConfig;
import com.ctriposs.lcache.LCache;
import com.ctriposs.lcache.utils.DateFormatter;
import com.ctriposs.lcache.utils.TestUtil;

public class LimitTest {
	
	public static void main(String args[]) {
		LCache cache = new LCache(CacheConfig.SMALL);
		
		String rndString = TestUtil.randomString(10);
		
		System.out.println("Start from date " + DateFormatter.formatCurrentDate());
		long start = System.currentTimeMillis();
		for(long counter = 0;; counter++) {
			cache.put(String.valueOf(counter).getBytes(), rndString.getBytes());
			if (counter%1000000 == 0) {
				System.out.println("Current date " + DateFormatter.formatCurrentDate());
				System.out.println(""+counter);
				System.out.println(TestUtil.printMemoryFootprint());
				long end = System.currentTimeMillis();
				System.out.println("timeSpent = " + (end - start));
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// ignore
				}
				start = System.currentTimeMillis();
			}
			
		}
	}

}