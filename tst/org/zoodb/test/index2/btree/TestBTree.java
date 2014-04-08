package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.btree.*;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.util.Pair;
import org.zoodb.tools.ZooConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestBTree {

	@Test
	public void searchSingleNode() {
		final int order = 10;
        BTreeFactory factory = factory(order, newBufferManager());
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(order / 2);
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
		final int order = 10000;
        BTreeFactory factory = factory(order, newBufferManager());
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(order);
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
		final int order = 10000;
        BTreeFactory factory = factory(order, newBufferManager());
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(order / 2);
		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		int[] missingKeys = { -1, order + 1 };
		for (int key : missingKeys) {
			assertEquals(
					"Incorrect return value when searching for missing key.",
					-1, tree.search(key));
		}
	}

	@Test
	public void searchMissingAfterSplit() {
		final int order = 10000;
        BTreeFactory factory = factory(order, newBufferManager());
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.increasingKeysRandomValues(order);
		for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		int[] missingKeys = { -1, order + 1 };
		for (int key : missingKeys) {
			assertEquals(
					"Incorrect return value when searching for missing key.",
					-1, tree.search(key));
		}
	}

	@Test
	public void insertWithSimpleSplit() {
		int order = 5;
        BTreeFactory factory = factory(order, newBufferManager());
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		tree.insert(3, 1);
		tree.insert(2, 5);
		tree.insert(0, 5);
		tree.insert(4, 10);
		tree.insert(1, -100);

		// build expected tree
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L)));
		factory.addLeafLayer(Arrays.asList(
				Arrays.asList(pair(0L, 5L), pair(1L, -100L), pair(2L, 5L)),
				Arrays.asList(pair(3L, 1L), pair(4L, 10L))));
		UniquePagedBTree expected = (UniquePagedBTree) factory.getTree();

		assertEquals(
				"Tree does not have the proper structure after insertion ",
				expected, tree);
	}

	/*
	 * Insert Test according to Silberschatz, Database System Concepts, Sixth
	 * edition
	 */
	@Test
	public void insertTwoLevelWithSplit() {
		int order = 4;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(90L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(40L, 60L),
				Arrays.asList(110L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(10L, 20L, 30L),
				Arrays.asList(40L, 50L), Arrays.asList(60L, 70L, 80L),
				Arrays.asList(90L, 100L), Arrays.asList(110L, 120L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(90L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(20L, 40L, 60L),
				Arrays.asList(110L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(5L, 10L),
				Arrays.asList(20L, 30L), Arrays.asList(40L, 50L),
				Arrays.asList(60L, 70L, 80L), Arrays.asList(90L, 100L),
				Arrays.asList(110L, 120L)));
		UniquePagedBTree expected = (UniquePagedBTree) factory.getTree();

		tree.insert(5, 5);
		assertEquals("Tree did not split properly after first insert.",
				expected, tree);

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(60L, 90L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(20L, 40L),
				Arrays.asList(80L), Arrays.asList(110L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(5L, 10L),
				Arrays.asList(20L, 30L), Arrays.asList(40L, 50L),
				Arrays.asList(60L, 70L), Arrays.asList(80L, 85L),
				Arrays.asList(90L, 100L), Arrays.asList(110L, 120L)));

		tree.insert(85, 85);

		expected = (UniquePagedBTree) factory.getTree();
		assertEquals("Tree did not split properly after second insert.",
				expected, tree);
	}

	/*
	 * Insert Test according to Ramakrishnan, Gehrke, Database Management
	 * Systems, Third edition
	 */
	@Test
	public void insertOneLevelWithSplit() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(13L, 17L, 24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(
				Arrays.asList(2L, 3L, 5L, 7L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(7L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L, 5L),
				Arrays.asList(7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree expected = (UniquePagedBTree) factory.getTree();

		tree.insert(8, 8);
		assertEquals("Tree did not split properly after insert.", expected,
				tree);
	}

	/*
	 * Delete Test according to Ramakrishnan, Gehrke, Database Management
	 * Systems, Third edition
	 */
	@Test
	public void deleteSimpleTest() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree expectedTree = (UniquePagedBTree) factory.getTree();

		tree.delete(19);
		assertEquals(expectedTree, tree);
	}

	@Test
	public void deleteRedistributeTest() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(27L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(22L, 24L), Arrays.asList(27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree expectedTree = (UniquePagedBTree) factory.getTree();

		tree.delete(20);
		assertEquals(expectedTree, tree);
	}

	@Test
	public void deleteMergeTest() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(27L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(22L, 24L), Arrays.asList(27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L, 17L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(22L, 27L, 29L), Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree expectedTree = (UniquePagedBTree) factory.getTree();

		tree.delete(24);
		assertEquals(expectedTree, tree);
	}

	/*
	 * Delete Test according to Silberschatz, Database System Concepts, Sixth
	 * edition
	 */
	@Test
	public void deleteMergeTest2() {
		int order = 4;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L, 5L, 7L),
				Arrays.asList(12L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(3L, 4L), Arrays.asList(5L, 6L),
				Arrays.asList(7L, 8L, 9L), Arrays.asList(10L, 11L),
				Arrays.asList(12L, 13L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(7L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L, 5L),
				Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(3L, 4L), Arrays.asList(5L, 6L),
				Arrays.asList(7L, 8L, 9L), Arrays.asList(10L, 11L, 13L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(12L);
		assertEquals(tree2, tree1);

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(7L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L, 5L),
				Arrays.asList(9L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(3L, 4L), Arrays.asList(5L, 6L),
				Arrays.asList(7L, 8L), Arrays.asList(9L, 10L)));
		UniquePagedBTree tree3 = (UniquePagedBTree) factory.getTree();
		tree2.delete(11L);
		tree2.delete(13L);
		assertEquals(tree3, tree2);

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L, 5L, 8L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(3L, 4L), Arrays.asList(5L, 6L),
				Arrays.asList(8L, 9L, 10L)));
		UniquePagedBTree tree4 = (UniquePagedBTree) factory.getTree();
		tree3.delete(7L);
		assertEquals(tree4, tree3);
	}

	@Test
	public void deleteMergeRight() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L, 3L),
				Arrays.asList(5L, 9L), Arrays.asList(10L, 11L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L, 3L),
				Arrays.asList(5L, 9L, 11L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(10L);
		assertEquals(tree2, tree1);
	}

	@Test
	public void deleteMergeLeft() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(5L, 9L), Arrays.asList(10L, 11L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L, 9L),
				Arrays.asList(10L, 11L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(5L);
		assertEquals(tree2, tree1);
	}

	@Test
	public void deleteRedistributeRightOdd() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(10L, 11L, 12L, 13L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(12L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 10L, 11L),
				Arrays.asList(12L, 13L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(2L);
		assertEquals(tree2, tree1);
	}

	@Test
	public void deleteRedistributeRightEven() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(10L, 11L, 12L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(11L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 10L),
				Arrays.asList(11L, 12L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(2L);
		assertEquals(tree2, tree1);
	}

	@Test
	public void deleteRedistributeLeftOdd() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(
				Arrays.asList(1L, 2L, 3L, 4L), Arrays.asList(10L, 11L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(3L, 4L, 11L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(10L);
		assertEquals(tree2, tree1);
	}

	@Test
	public void deleteRedistributeLeftEven() {
		int order = 5;
		BTreeFactory factory = factory(order, newBufferManager());
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L, 3L),
				Arrays.asList(10L, 11L)));
		UniquePagedBTree tree1 = (UniquePagedBTree) factory.getTree();
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L, 2L),
				Arrays.asList(3L, 11L)));
		UniquePagedBTree tree2 = (UniquePagedBTree) factory.getTree();
		tree1.delete(10L);
		assertEquals(tree2, tree1);
	}
	
	@Test
	public void anotherDeleteTest() {
		UniquePagedBTree tree = (UniquePagedBTree) getTestTree(newBufferManager());
		tree.delete(2);
		tree.delete(3);
		tree.delete(33);
		tree.delete(34);
		tree.delete(39);
		tree.delete(24);
		tree.delete(14);
		tree.delete(19);
	}


	/*
	 * Tests whether the state of the tree is correct after doing a lot of
	 * inserts and deletes.
	 */
	@Test
	public void testDeleteMassively() {
		int order = 320;
        BTreeFactory factory = factory(order, newBufferManager());
		deleteMassively(factory);
	}
	
	@Test
	public void testDeleteMassivelyWithDifferentOrder() {
		int leafOrder = 128;
		int innerOrder = 256;
        BTreeFactory factory = new BTreeFactory(innerOrder, leafOrder, newBufferManager(), true);
		deleteMassively(factory);
	}
	
	public void deleteMassively(BTreeFactory factory) {
		int numEntries = 100000;
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries);

		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		// check whether all entries are inserted
		for (LLEntry entry : entries) {
			assertEquals(entry.getValue(), tree.search(entry.getKey()));
		}

		// delete every entry and check that there is indeed no entry anymore
		for (LLEntry entry : entries) {
			tree.delete(entry.getKey());
		}
		for (LLEntry entry : entries) {
			assertEquals(-1, tree.search(entry.getKey()));
		}
		// root is empty and has no children
		assertEquals(0, tree.getRoot().getNumKeys());
		BTreeNode[] emptyChildren = new BTreeNode[tree.getRoot().getOrder()];
		Arrays.fill(emptyChildren, null);
		assertArrayEquals(emptyChildren, tree.getRoot().getChildren());

		// add all entries, delete half of it, check that correct ones are
		// deleted and still present respectively
		int split = numEntries / 2;
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
				assertEquals(-1, tree.search(entry.getKey()));
			} else {
				assertEquals(entry.getValue(), tree.search(entry.getKey()));
			}
			i++;
		}
	}

	@Test
	public void markDirtyTest() {
		BTreeBufferManager bufferManager = newBufferManager();
		UniquePagedBTree tree = (UniquePagedBTree) getTestTree(bufferManager);
		PagedBTreeNode root = tree.getRoot();
		assertTrue(root.isDirty());
		bufferManager.write( tree.getRoot());
		assertFalse(root.isDirty());

		tree.insert(4, 4);

		PagedBTreeNode lvl1child1 = (PagedBTreeNode) root.getChild(0);
		PagedBTreeNode lvl2child1 = (PagedBTreeNode) lvl1child1.getChild(0);
		assertTrue(root.isDirty());
		assertTrue(lvl1child1.isDirty());
		assertTrue(lvl2child1.isDirty());

		PagedBTreeNode lvl2child2 = (PagedBTreeNode) lvl1child1.getChild(1);
		PagedBTreeNode lvl2child3 = (PagedBTreeNode) lvl1child1.getChild(2);
		PagedBTreeNode lvl1child2 = (PagedBTreeNode) root.getChild(1);
		PagedBTreeNode lvl2child4 = (PagedBTreeNode) lvl1child2.getChild(0);
		PagedBTreeNode lvl2child5 = (PagedBTreeNode) lvl1child2.getChild(1);
		PagedBTreeNode lvl2child6 = (PagedBTreeNode) lvl1child2.getChild(2);
		assertFalse(lvl2child2.isDirty());
		assertFalse(lvl2child3.isDirty());
		assertFalse(lvl1child2.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());
		assertFalse(lvl2child6.isDirty());

		bufferManager.write(root);

		tree.insert(32, 32);
		PagedBTreeNode lvl2child7 = (PagedBTreeNode) lvl1child2.getChild(3);
		assertTrue(root.isDirty());
		assertTrue(lvl1child2.isDirty());
		assertTrue(lvl2child6.isDirty());
		assertTrue(lvl2child7.isDirty());
		assertFalse(lvl1child1.isDirty());
		assertFalse(lvl2child1.isDirty());
		assertFalse(lvl2child2.isDirty());
		assertFalse(lvl2child3.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());

		bufferManager.write(root);
		tree.delete(16);
		assertTrue(root.isDirty());
		assertTrue(lvl1child1.isDirty());
		assertFalse(lvl1child2.isDirty());
		assertFalse(lvl2child1.isDirty());
		assertTrue(lvl2child2.isDirty());
		assertTrue(lvl2child3.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());
		assertFalse(lvl2child6.isDirty());
		assertFalse(lvl2child7.isDirty());

		bufferManager.write(root);
		tree.delete(14);
		assertTrue(root.isDirty());
		assertTrue(lvl1child1.isDirty());
		assertTrue(lvl1child2.isDirty());
		assertFalse(lvl2child1.isDirty());
		assertTrue(lvl2child2.isDirty());
		assertTrue(lvl2child3.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());
		assertFalse(lvl2child6.isDirty());
		assertFalse(lvl2child7.isDirty());
	}

	@Test
	public void closeTest() {
		BTreeStorageBufferManager bufferManager = (BTreeStorageBufferManager) newBufferManager();

		UniquePagedBTree tree = (UniquePagedBTree) getTestTree(bufferManager);

		// build list of initial nodes
		ArrayList<Integer> nodeList = new ArrayList<Integer>();
		BTreeIterator iterator = new BTreeIterator(tree);
		while (iterator.hasNext()) {
			nodeList.add(((PagedBTreeNode) iterator.next()).getPageId());
		}
		
		tree.delete(2);
		tree.delete(3);
		closeTestHelper(tree, nodeList, bufferManager);

		tree.delete(5);
		tree.delete(7);
		tree.delete(8);
		closeTestHelper(tree, nodeList, bufferManager);
		
        tree.delete(24);
		tree.delete(27);
		tree.delete(29);
		tree.delete(33);
		closeTestHelper(tree, nodeList, bufferManager);

		tree.delete(14);
		tree.delete(16);
		closeTestHelper(tree, nodeList, bufferManager);
		
		tree.delete(19);
		tree.delete(20);
		tree.delete(22);
		closeTestHelper(tree, nodeList, bufferManager);

	}

	// test whether all of the nodes that are not in the tree anymore are also
	// not anymore present in the BufferManager
	private void closeTestHelper(UniquePagedBTree tree,
			ArrayList<Integer> nodeList, BTreeStorageBufferManager bufferManager) {

		ArrayList<Integer> removedNodeList = new ArrayList<Integer>(nodeList);
		
		BTreeIterator iterator = new BTreeIterator(tree);
		while (iterator.hasNext()) {
			Integer nodeId = ((PagedBTreeNode) iterator.next()).getPageId();
			removedNodeList.remove(nodeId);
		}
		for (int nodeId : removedNodeList) {
			assertEquals(null, bufferManager.readNodeFromMemory(nodeId));
		}
	}


	public static BTree<PagedBTreeNode> getTestTree(BTreeBufferManager bufferManager) {
		int order = 5;
		BTreeFactory factory = factory(order, bufferManager);
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
		return new Pair<Long, Long>(x, y);
	}

    private static BTreeFactory factory(int order, BTreeBufferManager bufferManager) {
        boolean unique = true;
        return new BTreeFactory(order, bufferManager, unique);
    }
    
    public static BTreeBufferManager newBufferManager() { 
    	StorageChannel storage = new StorageRootInMemory(ZooConfig.getFilePageSize());
    	boolean isUnique = true;
		return new BTreeStorageBufferManager(storage, isUnique);
    }
}
