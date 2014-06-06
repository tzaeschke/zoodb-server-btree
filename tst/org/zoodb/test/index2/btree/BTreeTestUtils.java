/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 *
 * This file is part of ZooDB.
 *
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 *
 * See the README and COPYING files for further information.
 */
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
		// ensure that entries with equal keys can not exists in the set
		Set<LLEntry> randomEntryList = new TreeSet<>(
				new Comparator<LLEntry>() {
					public int compare(LLEntry e1, LLEntry e2) {
						return Long.compare(e1.getKey(), e2.getKey());
					}
				});
		Random prng = new Random(seed);
		while (randomEntryList.size() < numElements) {
			randomEntryList.add(new LLEntry(prng.nextInt(2 * numElements), prng.nextInt(Integer.MAX_VALUE)));
		}
		return new ArrayList<>(randomEntryList);
	}
}
