package com.ctriposs.lcache;

import com.ctriposs.lcache.LCache;
import com.ctriposs.lcache.utils.TestUtil;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class LCacheTest {

	// You can set the STRESS_FACTOR system property to make the tests run more iterations.
	public static final double STRESS_FACTOR = Double.parseDouble(System.getProperty("STRESS_FACTOR", "1.0"));

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

		// Make sure FileStatsCollector has enough time to finish up its work.
		try {
			Thread.sleep(10 * 1000);
		} catch (InterruptedException e) {
		}

	}



	@After
	public void clear() throws IOException {
		if (cache != null) {
			cache.close();
		}
	}
}
