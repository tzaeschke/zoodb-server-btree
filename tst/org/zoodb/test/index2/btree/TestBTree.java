package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.util.Pair;

public class TestBTree {

	@Test
	public void searchSingleNode() {
		final int order = 10;
		BTree tree = new BTree(order);

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.randomUniformKeyValues(order / 2);
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
		BTree tree = new BTree(order);

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.randomUniformKeyValues(order);
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
		BTree tree = new BTree(order);

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.randomUniformKeyValues(order / 2);
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
		BTree tree = new BTree(order);

		Map<Long, Long> keyValueMap = BTreeTestUtils
				.randomUniformKeyValues(order);
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
		BTree tree = new BTree(order);
		tree.insert(3, 1);
		tree.insert(2, 5);
		tree.insert(0, 5);
		tree.insert(4, 10);
		tree.insert(1, -100);

		// build expected tree
		BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L)));
		factory.addLeafLayer(Arrays.asList(Arrays.asList(pair(0L,5L),pair(1L,-100L),pair(2L,5L)),
											Arrays.asList(pair(3L,1L),pair(4L,10L))));
		BTree expected = factory.getTree();
		
		assertEquals(
				"Tree does not have the proper structure after insertion ",
				expected, tree);
	}

	/*
	 * Insert Test according to Silberschatz, Database System Concepts, Sixth edition
	 */
	@Test
	public void insertTwoLevelWithSplit() {
		int order = 4;
		BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(90L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(40L, 60L),
								Arrays.asList(110L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(10L,20L,30L),
											Arrays.asList(40L,50L),
											Arrays.asList(60L,70L,80L),
											Arrays.asList(90L,100L),
											Arrays.asList(110L,120L)));
		BTree tree = factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(90L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(20L,40L,60L),
								Arrays.asList(110L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(5L,10L),
											Arrays.asList(20L,30L),
											Arrays.asList(40L,50L),
											Arrays.asList(60L,70L,80L),
											Arrays.asList(90L,100L),
											Arrays.asList(110L,120L)));
		BTree expected = factory.getTree();

		tree.insert(5, 5);
		assertEquals("Tree did not split properly after first insert.",
				expected, tree);
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(60L,90L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(20L,40L),
								Arrays.asList(80L),
								Arrays.asList(110L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(5L,10L),
											Arrays.asList(20L,30L),
											Arrays.asList(40L,50L),
											Arrays.asList(60L,70L),
											Arrays.asList(80L,85L),
											Arrays.asList(90L,100L),
											Arrays.asList(110L,120L)));

		tree.insert(85, 85);

		expected = factory.getTree();
		assertEquals("Tree did not split properly after second insert.",
				expected, tree);
	}
	
	/*
	 * Insert Test according to Ramakrishnan, Gehrke, Database Management Systems, Third edition
	 */
	@Test
	public void insertOneLevelWithSplit() {
		int order = 5;
        BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(13L,17L,24L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L,5L,7L),
											Arrays.asList(14L,16L),
											Arrays.asList(19L,20L,22L),
											Arrays.asList(24L,27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree tree = factory.getTree();

		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(7L,13L),
											Arrays.asList(24L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L,5L),
											Arrays.asList(7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(19L,20L,22L),
											Arrays.asList(24L,27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree expected = factory.getTree();

		tree.insert(8, 8);
		System.out.println(tree);
		System.out.println(expected);
		assertEquals("Tree did not split properly after insert.",
				expected, tree);
	}
	
	/*
	 * Delete Test according to Ramakrishnan, Gehrke, Database Management Systems, Third edition
	 */
	@Test
	public void deleteSimpleTest() {
		int order = 5;
		BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L,13L),
											Arrays.asList(24L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L),
											Arrays.asList(5L,7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(19L,20L,22L),
											Arrays.asList(24L,27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree tree = factory.getTree();
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L,13L),
											Arrays.asList(24L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L),
											Arrays.asList(5L,7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(20L,22L),
											Arrays.asList(24L,27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree expectedTree = factory.getTree();
		
		tree.delete(19);
		assertEquals(expectedTree, tree);
	}
	
	@Test
	public void deleteRedistributeTest() {
		int order = 5;
		BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L,13L),
											Arrays.asList(24L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L),
											Arrays.asList(5L,7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(20L,22L),
											Arrays.asList(24L,27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree tree = factory.getTree();
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L,13L),
											Arrays.asList(27L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L),
											Arrays.asList(5L,7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(22L,24L),
											Arrays.asList(27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree expectedTree = factory.getTree();
		
		tree.delete(20);
		assertEquals(expectedTree, tree);
	}
	
	@Test
	public void deleteMergeTest() {
		int order = 5;
		BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L,13L),
											Arrays.asList(27L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L),
											Arrays.asList(5L,7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(22L,24L),
											Arrays.asList(27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree tree = factory.getTree();
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L,13L,17L,30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L,3L),
											Arrays.asList(5L,7L,8L),
											Arrays.asList(14L,16L),
											Arrays.asList(22L,27L,29L),
											Arrays.asList(33L,34L,38L,39L)));
		BTree expectedTree = factory.getTree();
		
		tree.delete(24);
		assertEquals(expectedTree, tree);
	}
	
	/*
	 * Delete Test according to Silberschatz, Database System Concepts, Sixth edition
	 */
	@Test
	public void deleteMergeTest2() {
		int order = 4;
		BTreeFactory factory = new BTreeFactory(order);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(10L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L,5L,7L),
											Arrays.asList(12L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L,2L),
											Arrays.asList(3L,4L),
											Arrays.asList(5L,6L),
											Arrays.asList(7L,8L,9L),
											Arrays.asList(10L,11L),
											Arrays.asList(12L,13L)));
		BTree tree1 = factory.getTree();
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(7L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L,5L),
											Arrays.asList(10L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L,2L),
											Arrays.asList(3L,4L),
											Arrays.asList(5L,6L),
											Arrays.asList(7L,8L,9L),
											Arrays.asList(10L,11L,13L)));
		BTree tree2 = factory.getTree();
		tree1.delete(12L);
		assertEquals(tree2, tree1);
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(7L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L,5L),
											Arrays.asList(9L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L,2L),
											Arrays.asList(3L,4L),
											Arrays.asList(5L,6L),
											Arrays.asList(7L,8L),
											Arrays.asList(9L,10L)));
		BTree tree3 = factory.getTree();
		tree2.delete(11L);
		tree2.delete(13L);
		assertEquals(tree3, tree2);
		
		factory.clear();
		factory.addInnerLayer(Arrays.asList(Arrays.asList(3L,5L,8L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(1L,2L),
											Arrays.asList(3L,4L),
											Arrays.asList(5L,6L),
											Arrays.asList(8L,9L,10L)));
		BTree tree4 = factory.getTree();
		tree3.delete(7L);
		assertEquals(tree4, tree3);
	}

	public static Pair<Long,Long> pair(long x, long y) {
		return new Pair<Long,Long>(x,y);
	}

}
