package org.zoodb.test.index2.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.PrimLongMap;
import org.zoodb.internal.util.PrimLongMapZ;

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
    	return randomUniqueEntries(numElements, seed, false);
    }

    /**
     * Generate an array of LLEntries having random keys and values.
     *
     * It is required that the resulting list contains NO duplicate keys.
     *
     * @param numElements
     * @param seed
     * @param isOrdered
     * @return list
     */
    public static List<LLEntry> randomUniqueEntries(int numElements, long seed, boolean isOrdered) {
		Random prng = new Random(seed);
		// ensure that entries with equal keys can not exists in the set
    	if (isOrdered) {
    		Set<LLEntry> randomEntryList = new TreeSet<>(
    				new Comparator<LLEntry>() {
    					@Override
						public int compare(LLEntry e1, LLEntry e2) {
    						return Long.compare(e1.getKey(), e2.getKey());
    					}
    				});
    		while (randomEntryList.size() < numElements) {
    			randomEntryList.add(new LLEntry(prng.nextInt(2 * numElements), prng.nextInt(Integer.MAX_VALUE)));
    		}
    		return new ArrayList<>(randomEntryList);
    	} else {
            PrimLongMapZ<Integer> map = new PrimLongMapZ<>();
            while (map.size() < numElements) {
                map.put(prng.nextInt(2 * numElements), prng.nextInt(Integer.MAX_VALUE));
            }
            List<LLEntry> randomEntryList = new ArrayList<>();
            for (PrimLongMap.PrimLongEntry<Integer> entry : map.entrySet())  {
                randomEntryList.add(new LLEntry(entry.getKey(), entry.getValue()));
            }

            Collections.shuffle(randomEntryList);
            return randomEntryList;
    	}
	}
}
