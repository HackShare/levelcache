package com.ctriposs.lcache;

import java.io.IOException;

import com.ctriposs.lcache.CacheConfig;
import com.ctriposs.lcache.LCache;

public class ReadWriteTest {
	private static long TOTAL = 10000000;
	
	private static void write(LCache cache) {
		long sum = 0;
		long count = 0;
		long startTime = System.currentTimeMillis();
		
		for(long i = 0; i < TOTAL; i++) {
			byte[] keyBytes = ("" + i).getBytes();
			long oneStartTime = System.nanoTime();  
			cache.put(keyBytes, keyBytes);
			
			if (i % 100000 == 0) {
				sum += (System.nanoTime() - oneStartTime);
				count++;
			}
		}
		
        System.out.println("avg:" + sum / count + " ns");  
        System.out.println("write " + TOTAL / 1000000 + " million times:" + (System.currentTimeMillis() - startTime) + " ms");
	}
	
	private static void read(LCache cache) {
		long startTime = System.currentTimeMillis(); 
		for(long i = 0; i < TOTAL; i++) {
			byte[] keyBytes = ("" + i).getBytes();
			cache.get(keyBytes);
		}
		System.out.println("read " + TOTAL / 1000000 + " million times:" + (System.currentTimeMillis() - startTime) + " ms"); 
	}
	
	public static void main(String args[]) throws IOException {
		LCache cache = new LCache(CacheConfig.HUGE);
		
		write(cache);
		
		read(cache);
		
		cache.close();
	}
}
