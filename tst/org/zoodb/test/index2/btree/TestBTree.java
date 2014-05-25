package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
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

        Random random = new Random(Calendar.getInstance().getTimeInMillis());
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
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries, System.nanoTime());

		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		// check whether all entries are inserted
		for (LLEntry entry : entries) {
			assertEquals(new Long(entry.getValue()), tree.search(entry.getKey()));
		}

		// delete every entry and check that there is indeed no entry anymore
		for (LLEntry entry : entries) {
			tree.delete(entry.getKey());
		}
		for (LLEntry entry : entries) {
			assertEquals(null, tree.search(entry.getKey()));
		}

        Collections.shuffle(entries, new Random(Calendar.getInstance().getTimeInMillis()));
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
				assertEquals(new Long(entry.getValue()), tree.search(entry.getKey()));
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
        assertFalse(nonUniqueTree.insert(1, 2, true));
        assertTrue(nonUniqueTree.insert(2, 2, true));
        assertFalse(nonUniqueTree.insert(1, 0, true));
        assertFalse(nonUniqueTree.insert(1, 0, true));
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
    	StorageChannel storage = new StorageRootInMemory(pageSize);
    	boolean isUnique = true;
		return new BTreeStorageBufferManager(storage, isUnique);
    }
}
