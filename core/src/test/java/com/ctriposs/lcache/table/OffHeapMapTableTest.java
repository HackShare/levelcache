package com.ctriposs.lcache.table;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.ctriposs.lcache.merge.Level0Merger;
import com.ctriposs.lcache.table.GetResult;
import com.ctriposs.lcache.table.OffHeapMapTable;

public class OffHeapMapTableTest {

	private OffHeapMapTable mapTable;

	@Test
	public void testEmtpy() throws IOException, ClassNotFoundException {
		long createdTime = System.nanoTime();
		mapTable = new OffHeapMapTable(1, createdTime, 1000, 4 * AbstractMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		assertTrue(mapTable.getLevel() == 1);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());
		//assertTrue(mapTable.getBackFileSize() == (MMFMapTable.INIT_INDEX_FILE_SIZE + MMFMapTable.INIT_DATA_FILE_SIZE) * Level0Merger.DEFAULT_MERGE_WAYS);

		try {
			@SuppressWarnings("unused")
			GetResult result = mapTable.get("empty".getBytes());
			fail();
		} catch (IllegalArgumentException iae) {

		}

		try {
			mapTable.getMapEntry(-1);
			fail();
		} catch (IllegalArgumentException iae) {

		}

		try {
			mapTable.getMapEntry(0);
			fail();
		} catch (IllegalArgumentException iae) {

		}
	}

	@Test
	public void testAppendAndGet() throws IOException, ClassNotFoundException {
		long createdTime = System.nanoTime();
		mapTable = new OffHeapMapTable(1, createdTime, 1000, 4 * AbstractMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		assertTrue(mapTable.getLevel() == 1);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());
		//assertTrue(mapTable.getBackFileSize() == (MMFMapTable.INIT_INDEX_FILE_SIZE + MMFMapTable.INIT_DATA_FILE_SIZE) * Level0Merger.DEFAULT_MERGE_WAYS);

		mapTable.appendNew("key".getBytes(), "value".getBytes(), 500);
		assertTrue(mapTable.getLevel() == 1);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 1);
		assertFalse(mapTable.isEmpty());

		GetResult result = mapTable.get("key".getBytes());
		assertTrue(result.isFound());
		assertTrue(!result.isDeleted());
		assertTrue(!result.isExpired());

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		result = mapTable.get("key".getBytes());
		assertTrue(result.isFound());
		assertTrue(!result.isDeleted());
		assertTrue(result.isExpired());

		result = mapTable.get("key1".getBytes());
		assertFalse(result.isFound());

	}


	@Test
	public void testLoop() throws IOException, ClassNotFoundException {
		long createdTime = System.nanoTime();
		int loop =  32 * 1024;
		mapTable = new OffHeapMapTable(1, createdTime, loop, 4 * AbstractMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE);

		assertTrue(mapTable.getLevel() == 1);
		assertTrue(mapTable.getCreatedTime() == createdTime);
		assertTrue(mapTable.getAppendedItemCount() == 0);
		assertTrue(mapTable.isEmpty());


		List<byte[]> list = new ArrayList<byte[]>();
		for(int i = 0; i < loop; i++) {
			list.add(("key" + i).getBytes());
		}
		Collections.sort(list, new Comparator<byte[]>() {

			@Override
			public int compare(byte[] arg0, byte[] arg1) {
				int hash0 = Arrays.hashCode(arg0);
				int hash1 = Arrays.hashCode(arg1);
				if (hash0 < hash1) return -1;
				else if (hash0 > hash1) return 1;
				else return 0;
			}


		});

		for(int i = 0; i < loop; i++) {
			mapTable.appendNew(list.get(i), ("value" + i).getBytes(), -1);
		}

		assertTrue(mapTable.getAppendedItemCount() == loop);

		mapTable.truncate();
		assertTrue(mapTable.getTotalUsedOffHeapMemorySize() < (OffHeapMapTable.INIT_INDEX_OFFHEAP_MEMORY_SIZE + OffHeapMapTable.INIT_DATA_OFFHEAP_MEMORY_SIZE) * Level0Merger.DEFAULT_MERGE_WAYS);

		long start = System.currentTimeMillis();
		for(int i = 0; i < loop; i++) {
			GetResult result = mapTable.get(("key" + i).getBytes());
			assertTrue(result.isFound());
		}
		long time = System.currentTimeMillis() - start;
		System.out.printf("Get %,d K ops per second%n",
				(int) (loop / time));

		GetResult result = mapTable.get(("key" + loop).getBytes());
		assertFalse(result.isFound());
		assertFalse(result.isDeleted() || result.isExpired());

		mapTable.close();
	}

	@After
	public void clear() throws IOException {
		if (mapTable != null) {
			mapTable.close();
		}
	}

}
