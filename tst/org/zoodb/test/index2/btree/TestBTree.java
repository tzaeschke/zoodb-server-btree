package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.util.Pair;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

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
		BTree expected = new BTree(order);
		BTreeNode root = new BTreeNode(null, order, false);
		root.setNumKeys(1);
		root.setKeys(padLongArray(new long[] { 3 }, order));

		BTreeNode[] children = new BTreeNode[] {
				new BTreeNode(root, order, true),
				new BTreeNode(root, order, true) };
		root.setChildren(children);

		children[0].setKeys(padLongArray(new long[] { 0, 1, 2 }, order));
		children[0].setValues(padLongArray(new long[] { 5, -100, 5 }, order));
		children[0].setNumKeys(3);
		children[1].setKeys(padLongArray(new long[] { 3, 4 }, order));
		children[1].setValues(padLongArray(new long[] { 1, 10 }, order));
		children[1].setNumKeys(2);
		expected.setRoot(root);
		
		assertEquals(
				"Tree does not have the proper structure after insertion ",
				expected, tree);
	}

	/*
	 * Insert Test according to Silberschatz, Database System Concepts, Sixth edition
	 */
	@Test
	public void insertTwoLevelWithSplit() {
		BTree tree = treeInitialTwoLevelWithSplit();
		tree.insert(5, 5);
		BTree expected = treeExpectedTwoLevelWithSplit();
		assertEquals("Tree did not split properly after first insert.",
				expected, tree);
		tree.insert(85, 85);

		expected = treeExpectedFinalTwoLevelWithSplit();
		assertEquals("Tree did not split properly after second insert.",
				expected, tree);
	}

	public BTree treeInitialTwoLevelWithSplit() {
		int order = 4;
		BTree tree = new BTree(order);
		BTreeNode root = new BTreeNode(null, order, false);
		BTreeNode[] levelOneChildren = new BTreeNode[] {
				new BTreeNode(root, order, false),
				new BTreeNode(root, order, false) };
		root.setNumKeys(1);
		root.setKeys(padLongArray(new long[] { 90 }, order));
		root.setChildren(padChildrenArray(levelOneChildren, order));

		levelOneChildren[0].setNumKeys(2);
		levelOneChildren[0].setKeys(padLongArray(new long[] { 40, 60 }, order));

		levelOneChildren[1].setNumKeys(1);
		levelOneChildren[1].setKeys(padLongArray(new long[] { 110 }, order));

		BTreeNode[] leaves = new BTreeNode[] {
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[1], order, true),
				new BTreeNode(levelOneChildren[1], order, true) };

		levelOneChildren[0].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[0], leaves[1], leaves[2] }, order));
		levelOneChildren[1].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[3], leaves[4] }, order));

		leaves[0].setNumKeys(3);
		leaves[0].setKeys(padLongArray(new long[] { 10, 20, 30 }, order));

		leaves[1].setNumKeys(2);
		leaves[1].setKeys(padLongArray(new long[] { 40, 50 }, order));

		leaves[2].setNumKeys(3);
		leaves[2].setKeys(padLongArray(new long[] { 60, 70, 80 }, order));

		leaves[3].setNumKeys(2);
		leaves[3].setKeys(padLongArray(new long[] { 90, 100 }, order));

		leaves[4].setNumKeys(2);
		leaves[4].setKeys(padLongArray(new long[] { 110, 120 }, order));

		tree.setRoot(root);
		return tree;
	}

	public BTree treeExpectedTwoLevelWithSplit() {
		int order = 4;
		BTree tree = new BTree(order);
		BTreeNode root = new BTreeNode(null, order, false);
		BTreeNode[] levelOneChildren = new BTreeNode[] {
				new BTreeNode(root, order, false),
				new BTreeNode(root, order, false) };
		root.setNumKeys(1);
		root.setKeys(padLongArray(new long[] { 90 }, order));
		root.setChildren(padChildrenArray(levelOneChildren, order));

		levelOneChildren[0].setNumKeys(3);
		levelOneChildren[0].setKeys(padLongArray(new long[] { 20, 40, 60 },
				order));

		levelOneChildren[1].setNumKeys(1);
		levelOneChildren[1].setKeys(padLongArray(new long[] { 110 }, order));

		BTreeNode[] leaves = new BTreeNode[] {
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[1], order, true),
				new BTreeNode(levelOneChildren[1], order, true) };

		levelOneChildren[0].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[0], leaves[1], leaves[2], leaves[3] }, order));
		levelOneChildren[1].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[4], leaves[5] }, order));

		leaves[0].setNumKeys(2);
		leaves[0].setKeys(padLongArray(new long[] { 5, 10 }, order));
		leaves[0].setValues(padLongArray(new long[] { 5, 0, 0 }, order));

		leaves[1].setNumKeys(2);
		leaves[1].setKeys(padLongArray(new long[] { 20, 30 }, order));

		leaves[2].setNumKeys(2);
		leaves[2].setKeys(padLongArray(new long[] { 40, 50 }, order));

		leaves[3].setNumKeys(3);
		leaves[3].setKeys(padLongArray(new long[] { 60, 70, 80 }, order));

		leaves[4].setNumKeys(2);
		leaves[4].setKeys(padLongArray(new long[] { 90, 100 }, order));

		leaves[5].setNumKeys(2);
		leaves[5].setKeys(padLongArray(new long[] { 110, 120 }, order));

		tree.setRoot(root);
		return tree;
	}

	public BTree treeExpectedFinalTwoLevelWithSplit() {
		int order = 4;
		BTree tree = new BTree(order);
		BTreeNode root = new BTreeNode(null, order, false);
		BTreeNode[] levelOneChildren = new BTreeNode[] {
				new BTreeNode(root, order, false),
				new BTreeNode(root, order, false),
				new BTreeNode(root, order, false), };
		root.setNumKeys(2);
		root.setKeys(padLongArray(new long[] { 60, 90 }, order));
		root.setChildren(padChildrenArray(levelOneChildren, order));

		levelOneChildren[0].setNumKeys(2);
		levelOneChildren[0].setKeys(padLongArray(new long[] { 20, 40 }, order));

		levelOneChildren[1].setNumKeys(1);
		levelOneChildren[1].setKeys(padLongArray(new long[] { 80 }, order));

		levelOneChildren[2].setNumKeys(1);
		levelOneChildren[2].setKeys(padLongArray(new long[] { 110 }, order));

		BTreeNode[] leaves = new BTreeNode[] {
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[0], order, true),
				new BTreeNode(levelOneChildren[1], order, true),
				new BTreeNode(levelOneChildren[1], order, true),
				new BTreeNode(levelOneChildren[2], order, true),
				new BTreeNode(levelOneChildren[2], order, true) };

		levelOneChildren[0].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[0], leaves[1], leaves[2] }, order));
		levelOneChildren[1].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[3], leaves[4] }, order));
		levelOneChildren[2].setChildren(padChildrenArray(new BTreeNode[] {
				leaves[5], leaves[6] }, order));

		leaves[0].setNumKeys(2);
		leaves[0].setKeys(padLongArray(new long[] { 5, 10 }, order));
		leaves[0].setValues(padLongArray(new long[] { 5, 0, 0 }, order));

		leaves[1].setNumKeys(2);
		leaves[1].setKeys(padLongArray(new long[] { 20, 30 }, order));

		leaves[2].setNumKeys(2);
		leaves[2].setKeys(padLongArray(new long[] { 40, 50 }, order));

		leaves[3].setNumKeys(2);
		leaves[3].setKeys(padLongArray(new long[] { 60, 70 }, order));

		leaves[4].setNumKeys(2);
		leaves[4].setKeys(padLongArray(new long[] { 80, 85 }, order));
		leaves[4].setValues(padLongArray(new long[] { 0, 85 }, order));

		leaves[5].setNumKeys(2);
		leaves[5].setKeys(padLongArray(new long[] { 90, 100 }, order));

		leaves[6].setNumKeys(2);
		leaves[6].setKeys(padLongArray(new long[] { 110, 120 }, order));

		tree.setRoot(root);
		return tree;
	}
	
	/*
	 * Insert Test according to Ramakrishnan, Gehrke, Database Management Systems, Third edition
	 */
	@Test
	public void insertOneLevelWithSplit() {
		BTree tree = treeInitialOneLevelWithSplit();
		tree.insert(8, 8);
		BTree expected = treeExpectedOneLevelWithSplit();
		assertEquals("Tree did not split properly after insert.",
				expected, tree);
	}
	
	private BTree treeInitialOneLevelWithSplit() {
		int order = 5;
		BTree tree = new BTree(order);
		BTreeNode root = new BTreeNode(null, order, false);
		BTreeNode[] levelOneChildren = new BTreeNode[] {
				new BTreeNode(root, order, true),
				new BTreeNode(root, order, true),
				new BTreeNode(root, order, true),
				new BTreeNode(root, order, true),
				new BTreeNode(root, order, true) };

		root.setNumKeys(4);
		root.setKeys(padLongArray(new long[] { 13, 17, 24, 30 }, order));
		root.setChildren(padChildrenArray(levelOneChildren, order));

		levelOneChildren[0].setNumKeys(4);
		long[] leafOneRecords = new long[] { 2, 3, 5, 7 };
		levelOneChildren[0].setKeys(padLongArray(leafOneRecords, order));
		levelOneChildren[0].setValues(padLongArray(leafOneRecords, order));

		levelOneChildren[1].setNumKeys(2);
		long[] leafTwoRecords = new long[] { 14, 16 };
		levelOneChildren[1].setKeys(padLongArray(leafTwoRecords, order));
		levelOneChildren[1].setValues(padLongArray(leafTwoRecords, order));

		levelOneChildren[2].setNumKeys(3);
		long[] leafThreeRecords = new long[] { 19,20,22 };
		levelOneChildren[2].setKeys(padLongArray(leafThreeRecords, order));
		levelOneChildren[2].setValues(padLongArray(leafThreeRecords, order));
		
		levelOneChildren[3].setNumKeys(3);
		long[] leafFourRecords = new long[] { 24,27,29 };
		levelOneChildren[3].setKeys(padLongArray(leafFourRecords, order));
		levelOneChildren[3].setValues(padLongArray(leafFourRecords, order));
		
		levelOneChildren[4].setNumKeys(4);
		long[] leafFiveRecords = new long[] { 33,34,38,39 };
		levelOneChildren[4].setKeys(padLongArray(leafFiveRecords, order));
		levelOneChildren[4].setValues(padLongArray(leafFiveRecords, order));

		tree.setRoot(root);
		return tree;
	}
	
	private BTree treeExpectedOneLevelWithSplit() {
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

		return factory.getTree();

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

	private long[] padLongArray(long[] keys, int order) {
		long[] paddedKeyArray = new long[order - 1];
		for (int i = 0; i < keys.length; i++) {
			paddedKeyArray[i] = keys[i];
		}
		for (int i = keys.length; i < order - 1; i++) {
			paddedKeyArray[i] = 0;
		}
		return paddedKeyArray;
	}

	private BTreeNode[] padChildrenArray(BTreeNode[] children, int order) {
		BTreeNode[] paddedNodeArray = new BTreeNode[order];
		for (int i = 0; i < children.length; i++) {
			paddedNodeArray[i] = children[i];
		}
		for (int i = children.length; i < order; i++) {
			paddedNodeArray[i] = null;
		}
		return paddedNodeArray;
	}
	
	public static Pair<Long,Long> pair(long x, long y) {
		return new Pair<Long,Long>(x,y);
	}

}
