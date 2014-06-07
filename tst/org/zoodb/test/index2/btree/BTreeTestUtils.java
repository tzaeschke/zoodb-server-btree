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
    
    public static List<LLEntry> nonUniqueEntries(int numElements, int numTimes, int seed) {
        Random random = new Random(seed);
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
    	//TODO set this to true to let tests pass
    	return randomUniqueEntries(numElements, seed, false);
    }
    
    public static List<LLEntry> randomUniqueEntries(int numElements, long seed, boolean isOrdered) {
		// ensure that entries with equal keys can not exists in the set
    	if (isOrdered) {
    		Set<LLEntry> randomEntryList = new TreeSet<>(
    				new Comparator<LLEntry>() {
    					public int compare(LLEntry e1, LLEntry e2) {
    						return Long.compare(e1.getKey(), e2.getKey());
    					}
    				});
    		Random prng = new Random(seed);
    		int n = 0;
    		while (randomEntryList.size() < numElements) {
    			n++;
    			randomEntryList.add(new LLEntry(prng.nextInt(2 * numElements), prng.nextInt(Integer.MAX_VALUE)));
    		}
    		System.out.println("n=" + n);
    		System.out.println("l=" + randomEntryList.size());
    		return new ArrayList<>(randomEntryList);
    	} else {
    		HashSet<LLEntry> randomEntryList = new HashSet<>();
    		Random prng = new Random(seed);
    		int n = 0;
    		while (randomEntryList.size() < numElements) {
    			n++;
    			randomEntryList.add(new LLEntry(prng.nextInt(2 * numElements), prng.nextInt(Integer.MAX_VALUE)));
    		}
    		System.out.println("n=" + n);
    		ArrayList<LLEntry> xyz = new ArrayList<>(randomEntryList);
    		HashSet<LLEntry> hs2 = new HashSet<>(randomEntryList);
    		for (LLEntry e: xyz) {
    			LLEntry e2 = new LLEntry(e.getKey(), e.getValue());
    			n++;
    			if (!hs2.remove(e2)) {
    				throw new IllegalStateException();
    			}
    		}
    		System.out.println("n=" + n);
    		System.out.println("l=" + randomEntryList.size());
    		return new ArrayList<>(randomEntryList);
    	}
	}
}
