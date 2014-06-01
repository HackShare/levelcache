package com.ctriposs.lcache.table;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.lcache.table.AbstractMapTable;
import com.ctriposs.lcache.table.GetResult;
import com.ctriposs.lcache.table.HashMapTable;
import com.ctriposs.lcache.table.IMapEntry;

public class HashMapTableTest {

	private HashMapTable mapTable;

	@Test
	public void testEmtpy() throws IOException {
		long createdTime = System.nanoTime();
		mapTable = new HashMapTable((short)0, 0, createdTime);

		assertTrue(mapTable.getLevel() == 0);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());
		assertTrue(mapTable.getUsedIndexOffHeapMemorySize() == HashMapTable.INIT_INDEX_OFFHEAP_MEMORY_SIZE);
		assertTrue(mapTable.getUsedDataOffHeapMemorySize() == HashMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		GetResult result = mapTable.get("empty".getBytes());
		assertFalse(result.isFound());
		assertFalse(result.isDeleted() || result.isExpired());

		try {
			mapTable.getMapEntry(-1);
			fail();
		} catch (IllegalArgumentException iae) {

		}

		try {
			mapTable.getMapEntry(0);
		} catch (IllegalArgumentException iae) {

		}
	}

	@Test
	public void testAppendAndGet() throws IOException {
		long createdTime = System.nanoTime();
		mapTable = new HashMapTable(0, createdTime);

		assertTrue(mapTable.getLevel() == 0);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());
		assertTrue(mapTable.getUsedIndexOffHeapMemorySize() == HashMapTable.INIT_INDEX_OFFHEAP_MEMORY_SIZE);
		assertTrue(mapTable.getUsedDataOffHeapMemorySize() == HashMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		mapTable.appendNew("key".getBytes(), "value".getBytes(), 500, System.currentTimeMillis());
		assertTrue(mapTable.getLevel() == 0);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 1);
		assertFalse(mapTable.isEmpty());

		IMapEntry mapEntry = mapTable.getMapEntry(0);
		assertTrue(Arrays.equals("key".getBytes(), mapEntry.getKey()));
		assertTrue(Arrays.equals("value".getBytes(), mapEntry.getValue()));
		assertTrue(500 == mapEntry.getTimeToLive());
		assertTrue(System.currentTimeMillis() >= mapEntry.getCreatedTime());
		assertFalse(mapEntry.isDeleted());
		assertTrue(mapEntry.isInUse());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertTrue(mapEntry.isExpired());

		mapEntry.markDeleted();
		assertTrue(mapEntry.isDeleted());
	}

	@Test
	public void testMapOps() throws IOException {
		long createdTime = System.nanoTime();
		mapTable = new HashMapTable(0, createdTime);

		assertTrue(mapTable.getLevel() == 0);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());
		assertTrue(mapTable.getUsedIndexOffHeapMemorySize() == HashMapTable.INIT_INDEX_OFFHEAP_MEMORY_SIZE);
		assertTrue(mapTable.getUsedDataOffHeapMemorySize() == HashMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		for(int i = 0; i < 100; i++) {
			mapTable.put(("key"+i).getBytes(), ("value" + i).getBytes(), AbstractMapTable.NO_TIMEOUT, System.currentTimeMillis());
		}

		for(int i = 0; i < 100; i++) {
			if (i % 2 == 0) {
				mapTable.delete(("key" + i).getBytes());
			}
		}
		mapTable.delete(("key" + 100).getBytes());

		for(int i = 0; i < 100; i++) {
			GetResult result = mapTable.get(("key" + i).getBytes());
			if (i % 2 == 0) {
				assertTrue(result.isFound() && result.isDeleted());
			} else {
				assertTrue(result.isFound() && !result.isDeleted() && !result.isExpired());
				assertTrue(Arrays.equals(("value" + i).getBytes(), result.getValue()));
			}
		}

		GetResult result = mapTable.get(("key" + 100).getBytes());
		assertTrue(result.isFound() && result.isDeleted());
		result = mapTable.get(("key" + 101).getBytes());
		assertTrue(!result.isFound() && !result.isDeleted());

		mapTable.close();

		// test expiration
		createdTime = System.nanoTime();
		mapTable = new HashMapTable(0, createdTime);
		for(int i = 0; i < 100; i++) {
			mapTable.put(("key" + i).getBytes(), ("value" + i).getBytes(), 200, System.currentTimeMillis());
		}

		try {
			Thread.sleep(600);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		for(int i = 0; i < 100; i++) {
			result = mapTable.get(("key" + i).getBytes());
			assertTrue(result.isFound() && !result.isDeleted() && result.isExpired());
		}
	}

	@Test
	public void testLoop() throws IOException {
		long createdTime = System.nanoTime();
		mapTable = new HashMapTable(0, createdTime);

		assertTrue(mapTable.getLevel() == 0);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());
		assertTrue(mapTable.getUsedIndexOffHeapMemorySize() == HashMapTable.INIT_INDEX_OFFHEAP_MEMORY_SIZE);
		assertTrue(mapTable.getUsedDataOffHeapMemorySize() == HashMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		int loop = 6 * 1024;
		for(int i = 0; i < loop; i++) {
			mapTable.appendNew(("key" + i).getBytes(), ("value" + i).getBytes(), -1, System.currentTimeMillis());
		}

		assertTrue(mapTable.getAppendedItemCount()== loop);

		for(int i = 0; i < loop; i++) {
			IMapEntry mapEntry = mapTable.getMapEntry(i);
			
			//System.out.println(new String(mapEntry.getKey()));
			assertTrue(Arrays.equals(("key" + i).getBytes(), mapEntry.getKey()));
			assertTrue(Arrays.equals(("value" + i).getBytes(), mapEntry.getValue()));
			assertTrue(-1 == mapEntry.getTimeToLive());
			assertTrue(System.currentTimeMillis() >= mapEntry.getCreatedTime());
			assertFalse(mapEntry.isDeleted());
			assertTrue(mapEntry.isInUse());
		}

		for(int i = 0; i < loop; i++) {
			GetResult result = mapTable.get(("key" + i).getBytes());
			assertTrue(result.isFound());
			assertFalse(result.isDeleted() || result.isExpired());
			assertTrue(Arrays.equals(("value" + i).getBytes(),result.getValue()));
		}

		GetResult result = mapTable.get(("key" + loop).getBytes());
		assertFalse(result.isFound());
		assertFalse(result.isDeleted() || result.isExpired());

		try {
			mapTable.getMapEntry(loop);
		} catch (IllegalArgumentException iae) {

		}
	}

	@Test
	public void testAppendConcurrency() throws IOException, InterruptedException, ExecutionException {
		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		long createdTime = System.nanoTime();
		mapTable = new HashMapTable(1, createdTime);

		List<Future<?>> futures = new ArrayList<Future<?>>();

		int N_THREADS = 128;
		final int LOOP = 512;
		for(int t = 0; t < N_THREADS; t++) {
			final int finalT = t;
			futures.add(es.submit(new Runnable() {

				@Override
				public void run() {
					for(int i = 0; i < LOOP; i++) {
							mapTable.appendNew(("" + finalT).getBytes(), ("" + i).getBytes(), -1, System.currentTimeMillis());
					}

				}

			}));
		}

		for(Future<?> future : futures) {
			future.get();
		}

		assertTrue(mapTable.getAppendedItemCount() == N_THREADS * LOOP);

		Map<Integer, Set<Integer>> resultMap = new HashMap<Integer, Set<Integer>>();
		for(int i = 0; i < mapTable.getAppendedItemCount(); i++) {
			IMapEntry mapEntry = mapTable.getMapEntry(i);
			String key = new String(mapEntry.getKey());
			String value = new String(mapEntry.getValue());
			if (!resultMap.containsKey(Integer.parseInt(key))) {
				resultMap.put(Integer.parseInt(key), new HashSet<Integer>());
			}
			resultMap.get(Integer.parseInt(key)).add(Integer.parseInt(value));
		}

		assertTrue(resultMap.size() == N_THREADS);
		Set<Integer> keySet = resultMap.keySet();
		for(int t = 0; t < N_THREADS; t++) {
			keySet.contains(t);
		}
		for(Integer key : keySet) {
			Set<Integer> valueSet = resultMap.get(key);
			assertTrue(valueSet.size() == LOOP);
			for(int l = 0; l < LOOP; l++) {
				valueSet.contains(l);
			}
		}

		es.shutdown();
	}

	@After
	public void clear() throws IOException {
		if (mapTable != null) {
			mapTable.close();
		}
	}

}
