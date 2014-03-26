package org.zoodb.test.index2.btree;

import org.zoodb.internal.server.index.LongLongIndex.LLEntry;

import java.util.*;

public class BTreeTestUtils {

    public static Map<Long, Long> increasingKeysRandomValues(int numElements) {
        Map<Long, Long> map = new HashMap<Long, Long>();
        Random random = new Random();
        for (long i = 0; i < numElements; i++) {
            map.put(i, random.nextLong());
        }
        return map;
    }
    
    public static List<LLEntry> randomUniqueEntries(int numElements) {
		// ensure that entries with equal keys can not exists in the set
		Set<LLEntry> randomEntryList = new TreeSet<LLEntry>(
				new Comparator<LLEntry>() {
					public int compare(LLEntry e1, LLEntry e2) {
						return Long.compare(e1.getKey(), e2.getKey());
					}
				});
		//Random prng = new Random(System.nanoTime());
        Random prng = new Random(42);
		while (randomEntryList.size() < numElements) {
			randomEntryList.add(new LLEntry(prng.nextInt(Integer.MAX_VALUE), prng.nextInt(Integer.MAX_VALUE)));
		}
		return new ArrayList<LLEntry>(randomEntryList);
	}
}
