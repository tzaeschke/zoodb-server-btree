package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.util.Pair;
import org.zoodb.tools.ZooConfig;

public class TestNode {

    private static final int NO_VALUE = -1;

    IOResourceProvider storage = new StorageRootInMemory(
			ZooConfig.getFilePageSize()).createChannel();
	private BTreeNodeFactory nodeFactory = new PagedBTreeNodeFactory(
			new BTreeStorageBufferManager(storage, true));

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
		BTreeNode leafNode = nodeFactory.newUniqueNode( 6, true, true);
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

        innerNode.put(3, NO_VALUE, child1, child4);
		assertArrayEquals(new long[] { 3 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child4 },
				getChildren(innerNode));

        innerNode.put(1, NO_VALUE, child2);
		assertArrayEquals(new long[] { 1, 3 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child4 },
				getChildren(innerNode));

        innerNode.put(5, NO_VALUE, child6);
		assertArrayEquals(new long[] { 1, 3, 5 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child4, child6 },
				getChildren(innerNode));

        innerNode.put(2, NO_VALUE, child3);
		assertArrayEquals(new long[] { 1, 2, 3, 5 }, getKeys(innerNode));
		assertArrayEquals(new BTreeNode[] { child1, child2, child3, child4,
				child6 }, getChildren(innerNode));

        innerNode.put(4, NO_VALUE, child5);
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
        BTree tree = new UniquePagedBTree(4, new BTreeMemoryBufferManager());
		BTreeNode right = tree.putAndSplit(unevenKeysLeaf, 3, 3);
		assertArrayEquals(new long[] { 1, 2 }, getValues(unevenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(unevenKeysLeaf));
		assertArrayEquals(new long[] { 3, 4 }, getValues(right));
		assertArrayEquals(new long[] { 3, 4 }, getKeys(right));

		unevenKeysLeaf = nodeFactory.newUniqueNode(4, true, true);
		unevenKeysLeaf.put(1, 1);
		unevenKeysLeaf.put(3, 3);
		unevenKeysLeaf.put(4, 4);
		right = tree.putAndSplit(unevenKeysLeaf, 2,2);
		assertArrayEquals(new long[] { 1, 2 }, getValues(unevenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(unevenKeysLeaf));
		assertArrayEquals(new long[] { 3, 4 }, getValues(right));
		assertArrayEquals(new long[] { 3, 4 }, getKeys(right));

        tree = new UniquePagedBTree(5, new BTreeMemoryBufferManager());
		BTreeNode evenKeysLeaf = nodeFactory.newUniqueNode( 5, true, true);
		evenKeysLeaf.put(1, 1);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(4, 4);
		BTreeNode right2 = tree.putAndSplit(evenKeysLeaf, 5,5 );
		assertArrayEquals(new long[] { 1, 2, 3 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2, 3 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 4, 5 }, getValues(right2));
		assertArrayEquals(new long[] { 4, 5 }, getKeys(right2));

		evenKeysLeaf = nodeFactory.newUniqueNode( 5, true, true);
		evenKeysLeaf.put(1, 1);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(4, 4);
		evenKeysLeaf.put(5, 5);
		right2 = tree.putAndSplit(evenKeysLeaf, 3, 3);
		assertArrayEquals(new long[] { 1, 2, 3 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2, 3 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 4, 5 }, getValues(right2));
		assertArrayEquals(new long[] { 4, 5 }, getKeys(right2));

		evenKeysLeaf = nodeFactory.newUniqueNode(5, true, true);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(5, 5);
		evenKeysLeaf.put(7, 7);
		right2 = tree.putAndSplit(evenKeysLeaf, 8, 8);
		assertArrayEquals(new long[] { 2, 3, 5 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 2, 3, 5 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 7, 8 }, getValues(right2));
		assertArrayEquals(new long[] { 7, 8 }, getKeys(right2));

		evenKeysLeaf = nodeFactory.newUniqueNode(5, true, true);
		evenKeysLeaf.put(2, 2);
		evenKeysLeaf.put(3, 3);
		evenKeysLeaf.put(5, 5);
		evenKeysLeaf.put(7, 7);
		right2 = tree.putAndSplit(evenKeysLeaf, 1, 1);
		assertArrayEquals(new long[] { 1, 2, 3 }, getValues(evenKeysLeaf));
		assertArrayEquals(new long[] { 1, 2, 3 }, getKeys(evenKeysLeaf));
		assertArrayEquals(new long[] { 5, 7 }, getValues(right2));
		assertArrayEquals(new long[] { 5, 7 }, getKeys(right2));
	}

	@Test
	public void innerNodeSplit() {
		// uneven order
		int order = 3;
        BTree tree = new UniquePagedBTree(order, new BTreeMemoryBufferManager());
		BTreeNode node = tree.getNodeFactory().newUniqueNode(order, false, true);
		BTreeNode child1 = tree.getNodeFactory().newUniqueNode(order, true, true);
		BTreeNode child2 = tree.getNodeFactory().newUniqueNode(order, true, true);
		BTreeNode child3 = tree.getNodeFactory().newUniqueNode(order, true, true);
		BTreeNode child4 = tree.getNodeFactory().newUniqueNode(order, true, true);
		BTreeNode[] childArray = new BTreeNode[] { child1, child2, child3,
				child4 };

        node.put(3, NO_VALUE, child1, child3);
        node.put(4, NO_VALUE, child4);
		Pair<BTreeNode, Pair<Long, Long>> p = tree.putAndSplit(node, 2, NO_VALUE, child2);
		checkEvenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		node = tree.getNodeFactory().newUniqueNode(order, false, true);
		node.put(2, NO_VALUE, child1, child2);
		node.put(3, NO_VALUE, child3);
		p = tree.putAndSplit(node, 4, NO_VALUE, child4);
		checkEvenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		node = tree.getNodeFactory().newUniqueNode(order, false, true);
        node.put(3, NO_VALUE, child1, child3);
        node.put(4, NO_VALUE, child4);
		p = tree.putAndSplit(node, 2, NO_VALUE, child2);

		checkEvenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		// even order
		order = 4;
        tree = new UniquePagedBTree(order, new BTreeMemoryBufferManager());
		node = tree.getNodeFactory().newUniqueNode( order, false, true);
        child1 = tree.getNodeFactory().newUniqueNode(order, true, true);
        child2 = tree.getNodeFactory().newUniqueNode(order, true, true);
        child3 = tree.getNodeFactory().newUniqueNode(order, true, true);
        child4 = tree.getNodeFactory().newUniqueNode(order, true, true);
		BTreeNode child5 = tree.getNodeFactory().newUniqueNode(order, true, true);
		childArray = new BTreeNode[] { child1, child2, child3, child4, child5 };

        node.put(3, NO_VALUE, child1, child4);
        node.put(1, NO_VALUE, child2);
        node.put(4, NO_VALUE, child5);

		p = tree.putAndSplit(node, 2, NO_VALUE, child3);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		node = tree.getNodeFactory().newUniqueNode(order, false, true);
        node.put(3, NO_VALUE,child1, child4);
        node.put(1, NO_VALUE,child2);
        node.put(2, NO_VALUE,child3);
        tree.putAndSplit(node, 4, NO_VALUE, child5);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		node = tree.getNodeFactory().newUniqueNode(order, false, true);
        node.put(3, NO_VALUE, child1, child3);
        node.put(2, NO_VALUE, child3);
        node.put(4, NO_VALUE, child5);
        tree.putAndSplit(node, 1, NO_VALUE, child2);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);
	}
	
	@Test
	public void testDifferentOrders() {
		int innerOrder = 4;
		int leafOrder = 3;
        BTree tree = new UniquePagedBTree(innerOrder, leafOrder, new BTreeMemoryBufferManager());
        
        // test leaf
		BTreeNode leaf = tree.getNodeFactory().newUniqueNode( leafOrder, true, true);
		leaf.put(1, 1);
		leaf.put(3, 3);
		BTreeNode right = tree.putAndSplit(leaf, 2, 2);
		assertArrayEquals(new long[] { 1, 2 }, getValues(leaf));
		assertArrayEquals(new long[] { 1, 2 }, getKeys(leaf));
		assertArrayEquals(new long[] { 3 }, getValues(right));
		assertArrayEquals(new long[] { 3 }, getKeys(right));

		// test inner
		BTreeNode node = tree.getNodeFactory().newUniqueNode( innerOrder, false, true);
        BTreeNode child1 = tree.getNodeFactory().newUniqueNode(innerOrder, true, true);
        BTreeNode child2 = tree.getNodeFactory().newUniqueNode(innerOrder, true, true);
        BTreeNode child3 = tree.getNodeFactory().newUniqueNode(innerOrder, true, true);
        BTreeNode child4 = tree.getNodeFactory().newUniqueNode(innerOrder, true, true);
		BTreeNode child5 = tree.getNodeFactory().newUniqueNode(innerOrder, true, true);
		BTreeNode[] childArray = new BTreeNode[] { child1, child2, child3, child4, child5 };

        node.put(3, NO_VALUE, child1, child4);
        node.put(1, NO_VALUE, child2);
        node.put(4, NO_VALUE, child5);

		Pair<BTreeNode, Pair<Long, Long>> p = tree.putAndSplit(node, 2, NO_VALUE, child3);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		node = tree.getNodeFactory().newUniqueNode(innerOrder, false, true);
        node.put(3, NO_VALUE,child1, child4);
        node.put(1, NO_VALUE,child2);
        node.put(2, NO_VALUE,child3);
        tree.putAndSplit(node, 4, NO_VALUE, child5);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);

		node = tree.getNodeFactory().newUniqueNode(innerOrder, false, true);
        node.put(3, NO_VALUE, child1, child3);
        node.put(2, NO_VALUE, child3);
        node.put(4, NO_VALUE, child5);
        tree.putAndSplit(node, 1, NO_VALUE, child2);
		checkUnevenInnerNodeSplit(node, p.getA(), p.getB().getA(), childArray);
		
	}

	private void checkEvenInnerNodeSplit(BTreeNode node, BTreeNode right,
			long keyToMoveUp, BTreeNode[] childArray) {
		assertEquals(3, keyToMoveUp);
		assertArrayEquals(new long[] { 2 }, getKeys(node));
		assertArrayEquals(new BTreeNode[] { childArray[0], childArray[1] },
				getChildren(node));
		assertArrayEquals(new long[] { 4 }, getKeys(right));
		assertArrayEquals(new BTreeNode[] { childArray[2], childArray[3] },
				getChildren(right));
	}

	private void checkUnevenInnerNodeSplit(BTreeNode node, BTreeNode right,
			long keyToMoveUp, BTreeNode[] childArray) {
		assertEquals(3, keyToMoveUp);
		assertArrayEquals(new long[] { 1, 2 }, getKeys(node));
		assertArrayEquals(new BTreeNode[] { childArray[0], childArray[1],
				childArray[2] }, getChildren(node));
		assertArrayEquals(new long[] { 4 }, getKeys(right));
		assertArrayEquals(new BTreeNode[] { childArray[3], childArray[4] },
				getChildren(right));
	}

}
