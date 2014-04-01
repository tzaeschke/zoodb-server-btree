package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;
import org.zoodb.internal.util.Pair;
import org.zoodb.tools.ZooConfig;

public class TestNode {

	StorageChannel storage = new StorageRootInMemory(
			ZooConfig.getFilePageSize());
	private BTreeNodeFactory nodeFactory = new PagedBTreeNodeFactory(
			new BTreeStorageBufferManager(storage));

	public static long[] getKeys(BTreeNode node) {
		return Arrays.copyOfRange(node.getKeys(), 0, node.getNumKeys());
	}

	public static BTreeNode[] getChildren(BTreeNode innerNode) {
		return Arrays.copyOfRange(innerNode.getChildren(), 0,
				innerNode.getNumKeys() + 1);
	}

	public static long[] getValues(BTreeNode leafNode) {
		return Arrays.copyOfRange(leafNode.getValues(), 0,
				leafNode.getNumKeys());
	}

	@Test
	public void leafPut() {
		BTreeNode leafNode = nodeFactory.newUniqueNode(6, true, true);
		assertEquals(leafNode.getValues().length, 5);
		assertEquals(leafNode.getKeys().length, 5);

		leafNode.put(3, 3);
		assertArrayEquals(new long[] { 3 }, getKeys(leafNode));

		leafNode.put(1, 5);
		assertArrayEquals(new long[] { 1, 3 }, getKeys(leafNode));

		leafNode.put(5, 1);
		assertArrayEquals(new long[] { 1, 3, 5 }, getKeys(leafNode));

		leafNode.put(4, 2);
		assertArrayEquals(new long[] { 1, 3, 4, 5 }, getKeys(leafNode));

		leafNode.put(2, 4);
		assertArrayEquals(new long[] { 1, 2, 3, 4, 5 }, getKeys(leafNode));

		assertArrayEquals(new long[] { 5, 4, 3, 2, 1 }, getValues(leafNode));
	}

	@Test
	public void innerNodePut() {
		final int order = 6;
		BTreeNode innerNode = nodeFactory.newUniqueNode(order, false, true);
		assertEquals(5, innerNode.getKeys().length);
		assertEquals(order, innerNode.getChildren().length);

		BTreeNode child1 = nodeFactory.newUniqueNode(order, true, true);
		child1.put(1, 1);
		BTreeNode child2 = nodeFactory.newUniqueNode(order, true, true);
		child2.put(2, 2);
		BTreeNode child3 = nodeFactory.newUniqueNode(order, true, true);
		child3.put(3, 3);
		BTreeNode child4 = nodeFactory.newUniqueNode(order, true, true);
		child4.put(4, 4);
		BTreeNode child5 = nodeFactory.newUniqueNode(order, true, true);
		child5.put(5, 5);
		BTreeNode child6 = nodeFactory.newUniqueNode(order, true, true);
		child6.put(6, 6);

		System.out.println(innerNode);
		innerNode.put(3, child1, child4);
		System.out.println(innerNode);
		assertArrayEquals(new long[] { 3 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child4 },
				getChildren(innerNode));

		innerNode.put(1, child2);
		assertArrayEquals(new long[] { 1, 3 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child4 },
				getChildren(innerNode));

		innerNode.put(5, child6);
		assertArrayEquals(new long[] { 1, 3, 5 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child4, child6 },
				getChildren(innerNode));

		innerNode.put(2, child3);
		assertArrayEquals(new long[] { 1, 2, 3, 5 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child3, child4,
				child6 }, getChildren(innerNode));

		innerNode.put(4, child5);
		assertArrayEquals(new long[] { 1, 2, 3, 4, 5 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child3, child4,
				child5, child6 }, getChildren(innerNode));
	}

	@Test
	public void leafSplit() {
		BTreeNode unevenKeysLeaf = nodeFactory.newUniqueNode(4, true, true);
		unevenKeysLeaf.put(1, 1);
		unevenKeysLeaf.put(2, 2);
		unevenKeysLeaf.put(4, 4);
		BTreeNode right = unevenKeysLeaf.putAndSplit(3, 3);
		assertArrayEquals(new long[] { 1, 2 }, getValues(unevenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(unevenKeysLeaf));
		assertArrayEquals(new long[] { 3, 4 }, getValues(right));
		assertArrayEquals(new long[] { 3, 4 }, getKeys(right));

		unevenKeysLeaf = nodeFactory.newUniqueNode(4, true, true);
		unevenKeysLeaf.put(1, 1);
		unevenKeysLeaf.put(3, 3);
		unevenKeysLeaf.put(4, 4);
		right = unevenKeysLeaf.putAndSplit(2, 2);
		assertArrayEquals(new long[] { 1, 2 }, getValues(unevenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(unevenKeysLeaf));
		assertArrayEquals(new long[] { 3, 4 }, getValues(right));
		assertArrayEquals(new long[] { 3, 4 }, getKeys(right));

		BTreeNode evenKeysLeaf = nodeFactory.newUniqueNode(5, true, true);
		evenKeysLeaf.put(1, 1);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(4, 4);
		BTreeNode right2 = evenKeysLeaf.putAndSplit(5, 5);
		assertArrayEquals(new long[] { 1, 2, 3 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2, 3 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 4, 5 }, getValues(right2));
		assertArrayEquals(new long[] { 4, 5 }, getKeys(right2));

		evenKeysLeaf = nodeFactory.newUniqueNode(5, true, true);
		evenKeysLeaf.put(1, 1);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(4, 4);
		evenKeysLeaf.put(5, 5);
		right2 = evenKeysLeaf.putAndSplit(3, 3);
		assertArrayEquals(new long[] { 1, 2, 3 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2, 3 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 4, 5 }, getValues(right2));
		assertArrayEquals(new long[] { 4, 5 }, getKeys(right2));

		evenKeysLeaf = nodeFactory.newUniqueNode(5, true, true);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(5, 5);
		evenKeysLeaf.put(7, 7);
		right2 = evenKeysLeaf.putAndSplit(8, 8);
		assertArrayEquals(new long[] { 2, 3, 5 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 2, 3, 5 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 7, 8 }, getValues(right2));
		assertArrayEquals(new long[] { 7, 8 }, getKeys(right2));

		evenKeysLeaf = nodeFactory.newUniqueNode(5, true, true);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(5, 5);
		evenKeysLeaf.put(7, 7);
		right2 = evenKeysLeaf.putAndSplit(1, 1);
		assertArrayEquals(new long[] { 1, 2, 3 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2, 3 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 5, 7 }, getValues(right2));
		assertArrayEquals(new long[] { 5, 7 }, getKeys(right2));
	}

	@Test
	public void innerNodeSplit() {
		// uneven order
		int order = 3;
		BTreeNode node = nodeFactory.newUniqueNode(order, false, true);
		BTreeNode child1 = nodeFactory.newUniqueNode(order, true, true);
		BTreeNode child2 = nodeFactory.newUniqueNode(order, true, true);
		BTreeNode child3 = nodeFactory.newUniqueNode(order, true, true);
		BTreeNode child4 = nodeFactory.newUniqueNode(order, true, true);
		BTreeNode[] childArray = new BTreeNode[] { child1, child2, child3,
				child4 };

		node.put(3, child1, child3);
		node.put(4, child4);
		System.out.println(node);
		Pair<BTreeNode, Long> p = node.putAndSplit(2, child2);
		checkEvenInnerNodeSplit(node, p.getA(), p.getB(), childArray);

		node = nodeFactory.newUniqueNode(order, false, true);
		node.put(2, child1, child2);
		node.put(3, child3);
		p = node.putAndSplit(4, child4);
		checkEvenInnerNodeSplit(node, p.getA(), p.getB(), childArray);

		node = nodeFactory.newUniqueNode(order, false, true);
		node.put(3, child1, child3);
		node.put(4, child4);
		p = node.putAndSplit(2, child2);
		checkEvenInnerNodeSplit(node, p.getA(), p.getB(), childArray);

		// even order
		order = 4;
		node = nodeFactory.newUniqueNode(order, false, true);
		BTreeNode child5 = nodeFactory.newUniqueNode(order, true, true);
		childArray = new BTreeNode[] { child1, child2, child3, child4, child5 };

		node.put(3, child1, child4);
		node.put(1, child2);
		node.put(4, child5);
		p = node.putAndSplit(2, child3);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB(), childArray);

		node = nodeFactory.newUniqueNode(order, false, true);
		node.put(3, child1, child4);
		node.put(1, child2);
		node.put(2, child3);
		p = node.putAndSplit(4, child5);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB(), childArray);

		node = nodeFactory.newUniqueNode(order, false, true);
		node.put(3, child1, child4);
		node.put(2, child3);
		node.put(4, child5);
		p = node.putAndSplit(1, child2);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB(), childArray);
	}

    //TODO cleanup the calls to parent
	private void checkEvenInnerNodeSplit(BTreeNode node, BTreeNode right,
			long keyToMoveUp, BTreeNode[] childArray) {
		assertEquals(3, keyToMoveUp);
		assertArrayEquals(new long[] { 2 }, getKeys(node));
		assertArrayEquals(new BTreeNode[] { childArray[0], childArray[1] },
				getChildren(node));
//		assertTrue(childArray[0].getParent() == node
//				&& childArray[1].getParent() == node);
		assertArrayEquals(new long[] { 4 }, getKeys(right));
		assertArrayEquals(new BTreeNode[] { childArray[2], childArray[3] },
				getChildren(right));
//		assertTrue(childArray[2].getParent() == right
//				&& childArray[3].getParent() == right);
	}

	private void checkUnevenInnerNodeSplit(BTreeNode node, BTreeNode right,
			long keyToMoveUp, BTreeNode[] childArray) {
		assertEquals(3, keyToMoveUp);
		assertArrayEquals(new long[] { 1, 2 }, getKeys(node));
		assertArrayEquals(new BTreeNode[] { childArray[0], childArray[1],
				childArray[2] }, getChildren(node));
//		assertTrue(childArray[0].getParent() == node
//				&& childArray[1].getParent() == node
//				&& childArray[2].getParent() == node);
		assertArrayEquals(new long[] { 4 }, getKeys(right));
		assertArrayEquals(new BTreeNode[] { childArray[3], childArray[4] },
				getChildren(right));
//		assertTrue(childArray[3].getParent() == right
//				&& childArray[4].getParent() == right);
	}

}
