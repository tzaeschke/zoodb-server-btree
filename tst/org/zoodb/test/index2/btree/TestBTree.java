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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.PagedLongLong;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.util.Pair;
import org.zoodb.tools.ZooConfig;

public class TestBTree {

	@Test
	public void searchSingleNode() {
		int pageSize = 128;
        BTreeFactory factory = factory(newBufferManager(pageSize));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
        int numberOfElements = numberOfElementsFromPageSizeAvoidSplit(pageSize);
		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(numberOfElements);
		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			long expectedValue = entry.getValue();
			long value = tree.search(entry.getKey());
			assertEquals("Incorrect value retrieved.", expectedValue, value);
		}
	}

	@Test
	public void searchAfterSplit() {
		final int pageSize = 128;
        BTreeFactory factory = factory(newBufferManager(pageSize));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
        int numberOfElements = 320;

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(numberOfElements);
		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			long expectedValue = entry.getValue();
			long value = tree.search(entry.getKey());
			assertEquals("Incorrect value retrieved.", expectedValue, value);
		}
	}

	@Test
	public void searchMissingSingleNode() {
		final int pageSize = 128;
        BTreeFactory factory = factory(newBufferManager(pageSize));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

        int numberOfElements = 512;
		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(numberOfElements);
		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		int[] missingKeys = { -1, numberOfElements + 1 };
		for (int key : missingKeys) {
			assertEquals(
					"Incorrect return value when searching for missing key.",
					null, tree.search(key));
		}
	}

	@Test
	public void searchMissingAfterSplit() {
		final int pageSize = 1024;
        BTreeFactory factory = factory(newBufferManager(pageSize));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
        int numberOfElements = 100000;
		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(numberOfElements);
		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		int[] missingKeys = { -1, numberOfElements + 1 };
		for (int key : missingKeys) {
			assertEquals(
					"Incorrect return value when searching for missing key.",
					null, tree.search(key));
		}
	}

    @Test
    public void testInsertCheckAfterEachDelete() {
        final int pageSize = 128;
        BTreeFactory factory = factory(newBufferManager(pageSize));
        UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
        int numberOfElements = 10000;
        Map<Long, Long> keyValueMap = BTreeTestUtils
                .increasingKeysRandomValues(numberOfElements);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        Random random = new Random(System.currentTimeMillis());
        while (!keyValueMap.isEmpty()) {
            long key = random.nextInt(numberOfElements);
            if (keyValueMap.containsKey(key)) {
                tree.delete(key);
                keyValueMap.remove(key);
                for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
                    assertEquals(entry.getValue(), tree.search(entry.getKey()));
                }
            }
        }
    }
    
    @Test
	public void testInsertCase() {
		BTreeBufferManager bufferManager = newBufferManager(64);
		UniquePagedBTree tree = (UniquePagedBTree) getTestTreeWithThreeLayers(bufferManager);

		tree.insert(4, 4);
		tree.insert(32, 32);
		tree.insert(35, 35);
	}
	
    @Test
	public void testDeleteCase() {
		UniquePagedBTree tree = (UniquePagedBTree) getTestTreeWithThreeLayers(newBufferManager(ZooConfig.getFilePageSize()));

		tree.insert(4, 4);
		tree.insert(32, 32);
		tree.delete(16);
		tree.delete(14);
	}

    @Test
    public void testDeleteSmall() {
        int pageSize = 128;
        int numEntries = 128;
        BTreeFactory factory = factory(newBufferManager(pageSize));
        deleteMassively(factory, numEntries);
    }

	/*
	 * Tests whether the state of the tree is correct after doing a lot of
	 * inserts and deletes.
	 */
	@Test
	public void testDeleteMassively() {
		int pageSize = 128;
        int numEntries = 100000;
        BTreeFactory factory = factory(newBufferManager(pageSize));
		deleteMassively(factory, numEntries);
	}
	
	@Test
	public void testDeleteMassivelyWithDifferentOrder() {
		int pageSize = 256;
        int numEntries = 200000;
        BTreeFactory factory = new BTreeFactory(newBufferManager(pageSize), true);
		deleteMassively(factory, numEntries);
	}
	
	public void deleteMassively(BTreeFactory factory, int numEntries) {
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries, 0);

		for (LLEntry entry : entries) {
            if (tree.search(entry.getKey()) != null) {
                throw new IllegalStateException("Entry set contains duplicate keys.");
            }
			tree.insert(entry.getKey(), entry.getValue());
			assertEquals(entry.getValue(), (long)tree.search(entry.getKey()));
		}

		// check whether all entries are inserted
		for (LLEntry entry : entries) {
			assertEquals(entry.getValue(), (long)tree.search(entry.getKey()));
		}

		// delete every entry and check that there is indeed no entry anymore
		for (LLEntry entry : entries) {
			tree.delete(entry.getKey());
		}
		for (LLEntry entry : entries) {
			assertEquals(null, tree.search(entry.getKey()));
		}

        Collections.shuffle(entries, new Random(System.currentTimeMillis()));
        // root is empty and has no children
        assertEquals(0, tree.getRoot().getNumKeys());
        assertTrue(tree.getRoot().isLeaf());
        assertArrayEquals(null, tree.getRoot().getChildrenPageIds());

		// add all entries, delete a portion of it, check that correct ones are
		// deleted and still present respectively
		int split = (9*numEntries) / (10*numEntries);
		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}
		int i = 0;
		for (LLEntry entry : entries) {
			if (i < split) {
				tree.delete(entry.getKey());
			} else {
				break;
			}
			i++;
		}
		i = 0;
		for (LLEntry entry : entries) {
			if (i < split) {
				assertEquals(null, tree.search(entry.getKey()));
			} else {
				assertEquals(Long.valueOf(entry.getValue()), tree.search(entry.getKey()));
			}
			i++;
		}
		
		// there is only one root
		BTreeIterator it = new BTreeIterator(tree);
        assertTrue(it.next().isRoot());
		while(it.hasNext()) {
			assertFalse(it.next().isRoot());
		}
		
	}
	
	@Test
	public void testInsertLongIfNotSet() {
		int pageSize = 128;
        UniquePagedBTree uniqueTree = new UniquePagedBTree(pageSize, newBufferManager(pageSize));
        assertTrue(uniqueTree.insert(1, 1, true));
        assertFalse(uniqueTree.insert(1, 1, true));
        assertFalse(uniqueTree.insert(1, 2, true));
        assertTrue(uniqueTree.insert(2, 2, true));
        assertFalse(uniqueTree.insert(1, 1, true));
        
        NonUniquePagedBTree nonUniqueTree = new NonUniquePagedBTree(pageSize, newBufferManager(pageSize));
        assertTrue(nonUniqueTree.insert(1, 1, true));
        assertFalse(nonUniqueTree.insert(1, 1, true));
//        assertFalse(nonUniqueTree.insert(1, 2, true));
//        assertTrue(nonUniqueTree.insert(2, 2, true));
//        assertFalse(nonUniqueTree.insert(1, 0, true));
//        assertFalse(nonUniqueTree.insert(1, 0, true));
        assertTrue(nonUniqueTree.insert(1, 2, true));
        assertTrue(nonUniqueTree.insert(2, 2, true));
        assertTrue(nonUniqueTree.insert(1, 0, true));
        assertFalse(nonUniqueTree.insert(1, 0, true));
	}

	@Test
	public void testInsertLongIfNotSet_OLD_INDEX() {
		int pageSize = 128;
		IOResourceProvider paf = new StorageRootInMemory(pageSize).createChannel();
        PagedUniqueLongLong uniqueTree = new PagedUniqueLongLong(PAGE_TYPE.GENERIC_INDEX,paf);
        assertTrue(uniqueTree.insertLongIfNotSet(1, 1));
        assertFalse(uniqueTree.insertLongIfNotSet(1, 1));
        assertFalse(uniqueTree.insertLongIfNotSet(1, 2));
        assertTrue(uniqueTree.insertLongIfNotSet(2, 2));
        assertFalse(uniqueTree.insertLongIfNotSet(1, 1));
        
        PagedLongLong nonUniqueTree = new PagedLongLong(PAGE_TYPE.GENERIC_INDEX,paf);
        assertTrue(nonUniqueTree.insertLongIfNotSet(1, 1));
        assertFalse(nonUniqueTree.insertLongIfNotSet(1, 1));
        assertTrue(nonUniqueTree.insertLongIfNotSet(1, 2));
        assertTrue(nonUniqueTree.insertLongIfNotSet(2, 2));
        assertTrue(nonUniqueTree.insertLongIfNotSet(1, 0));
        assertFalse(nonUniqueTree.insertLongIfNotSet(1, 0));
	}

	public static PagedBTree getTestTreeWithThreeLayers(
			BTreeBufferManager bufferManager) {
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		return tree;
	}

	public static Pair<Long, Long> pair(long x, long y) {
		return new Pair<>(x, y);
	}

    private static BTreeFactory factory(BTreeBufferManager bufferManager) {
        boolean unique = true;
        return new BTreeFactory(bufferManager, unique);
    }

    private int numberOfElementsFromPageSizeAvoidSplit(int pageSize) {
        //just to be safe that all of the elements fit
        return pageSize / 16;
    }
    
    public static BTreeBufferManager newBufferManager() { 
    	int pageSize = ZooConfig.getFilePageSize();
		return newBufferManager(pageSize);
    }
    
    public static BTreeBufferManager newBufferManager(int pageSize) { 
    	IOResourceProvider storage = new StorageRootInMemory(pageSize).createChannel();
    	boolean isUnique = true;
		return new BTreeStorageBufferManager(storage, isUnique);
    }
}
