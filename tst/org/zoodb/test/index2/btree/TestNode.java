package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.util.Pair;

public class TestNode {

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
		BTreeNode leafNode = new BTreeNode(null, 6, true);
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
		BTreeNode innerNode = new BTreeNode(null, order, false);
		assertEquals(5, innerNode.getKeys().length);
		assertEquals(order, innerNode.getChildren().length);

		BTreeNode child1 = new BTreeNode(null, order, true);
		child1.put(1, 1);
		BTreeNode child2 = new BTreeNode(null, order, true);
		child2.put(2, 2);
		BTreeNode child3 = new BTreeNode(null, order, true);
		child3.put(3, 3);
		BTreeNode child4 = new BTreeNode(null, order, true);
		child4.put(4, 4);
		BTreeNode child5 = new BTreeNode(null, order, true);
		child5.put(5, 5);
		BTreeNode child6 = new BTreeNode(null, order, true);
		child6.put(6, 6);

		innerNode.put(3, child1, child4);
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
		BTreeNode unevenKeysLeaf = new BTreeNode(null, 4, true);
		unevenKeysLeaf.put(1, 1);
		unevenKeysLeaf.put(2, 2);
		unevenKeysLeaf.put(3, 3);

		BTreeNode right = unevenKeysLeaf.split();
		assertArrayEquals(new long[] { 1, 2 }, getValues(unevenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(unevenKeysLeaf));
		assertArrayEquals(new long[] { 3 }, getValues(right));
		assertArrayEquals(new long[] { 3 }, getKeys(right));

		BTreeNode evenKeysLeaf = new BTreeNode(null, 5, true);
		evenKeysLeaf.put(1, 1);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(4, 4);

		BTreeNode right2 = evenKeysLeaf.split();
		assertArrayEquals(new long[] { 1, 2 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 3, 4 }, getValues(right2));
		assertArrayEquals(new long[] { 3, 4 }, getKeys(right2));
	}
	
	@Test
	public void innerNodeSplit() {
		// uneven order
		int order = 3;
		BTreeNode node = new BTreeNode(null, order, false);
		BTreeNode child1 = new BTreeNode(null, order, true);
		BTreeNode child2 = new BTreeNode(null, order, true);
		BTreeNode child3 = new BTreeNode(null, order, true);
		BTreeNode child4 = new BTreeNode(null, order, true);

		node.put(3, child1, child3);
		node.put(4, child4);
		
		Pair<BTreeNode, Long> p = node.putAndSplit(2, child2);
		long keyToMoveUp = p.getB();
		BTreeNode right = p.getA();
		assertEquals(3, keyToMoveUp);
		assertArrayEquals(new long[]{2}, getKeys(node));
		assertArrayEquals(new BTreeNode[]{child1, child2}, getChildren(node));
		assertTrue(child1.getParent() == node 
						&& child2.getParent() == node);
		assertArrayEquals(new long[]{4}, getKeys(right));
		assertArrayEquals(new BTreeNode[]{child3, child4}, getChildren(right));
        assertTrue(child3.getParent() == right 
						&& child4.getParent() == right);
		
		// even order
		order = 4;
		node = new BTreeNode(null, order, false);
		BTreeNode child5 = new BTreeNode(null, order, true);

		node.put(3, child1, child4);
		node.put(1, child2);
		node.put(4, child5);
		
		p = node.putAndSplit(2, child3);
		keyToMoveUp = p.getB();
		right = p.getA();
		assertEquals(3, keyToMoveUp);
		assertArrayEquals(new long[]{1,2}, getKeys(node));
		assertArrayEquals(new BTreeNode[]{child1, child2, child3}, getChildren(node));
		assertTrue(child1.getParent() == node 
						&& child2.getParent() == node
						&& child3.getParent() == node);
		assertArrayEquals(new long[]{4}, getKeys(right));
		assertArrayEquals(new BTreeNode[]{child4, child5}, getChildren(right));
        assertTrue(child4.getParent() == right 
						&& child5.getParent() == right);
	}

}
