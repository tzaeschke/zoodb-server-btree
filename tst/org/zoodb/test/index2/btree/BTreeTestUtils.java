package org.zoodb.test.index2.btree;

import org.zoodb.internal.server.index.LongLongIndex.LLEntry;

import java.util.*;

public class BTreeTestUtils {

    public static Map<Long, Long> increasingKeysRandomValues(int numElements) {
        Map<Long, Long> map = new HashMap<>();
        Random random = new Random();
        for (long i = 0; i < numElements; i++) {
            map.put(i, random.nextLong());
        }
        return map;
    }
    
    public static List<LLEntry> nonUniqueEntries(int numElements, int numTimes) {
        Random random = new Random();
        Set<LLEntry> entrySet = new HashSet<>();
        for(int j=0; j<numTimes; j++) {
	        for (int i = 0; i < numElements; i++) {
                LLEntry newEntry = null;
                do {
                    newEntry = new LLEntry(i, random.nextLong());
                } while (entrySet.contains(newEntry));
                entrySet.add(newEntry);
	        }
        }
        return new ArrayList<>(entrySet);
    }
    
    public static List<LLEntry> randomUniqueEntries(int numElements, long seed) {
		// ensure that entries with equal keys can not exists in the set
		Set<LLEntry> randomEntryList = new TreeSet<>(
				new Comparator<LLEntry>() {
					public int compare(LLEntry e1, LLEntry e2) {
						return Long.compare(e1.getKey(), e2.getKey());
					}
				});
		Random prng = new Random(seed);
		while (randomEntryList.size() < numElements) {
			randomEntryList.add(new LLEntry(prng.nextInt(Integer.MAX_VALUE), prng.nextInt(Integer.MAX_VALUE)));
		}
		return new ArrayList<>(randomEntryList);
	}
}
