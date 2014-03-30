package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;
import org.zoodb.tools.ZooConfig;

public class TestBTreeStorageBufferManager {

	StorageChannel storage = new StorageRootInMemory(
			ZooConfig.getFilePageSize());

	@Test
	public void testWriteLeaf() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		int pageId = bufferManager.write(leafNode);

		assertEquals(leafNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		assertEquals(leafNode, bufferManager2.read(pageId));
		assertFalse(bufferManager2.read(pageId).isDirty());
	}

	@Test
	public void testWriteInnerNode() {
		int order = 3;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode innerNode = getTestInnerNode(bufferManager, order);
		int pageId = bufferManager.write(innerNode);

		assertEquals(innerNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		assertEquals(innerNode, bufferManager2.read(pageId));
		assertFalse(bufferManager2.read(pageId).isDirty());
	}

	@Test
	public void testWriteTree() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		BTree tree = getTestTree(bufferManager);
		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot());

		assertEquals(tree.getRoot(), bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		assertEquals(tree.getRoot(), bufferManager2.read(pageId));
		assertFalse(bufferManager2.read(pageId).isDirty());
	}

	@Test
	public void testDelete() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		int pageId = bufferManager.write(leafNode);

		bufferManager.delete(pageId);
		assertEquals(null, bufferManager.getMemoryBuffer().get(pageId));
		// assertEquals(null, bufferManager.read(pageId));
		// does not work because data is still on the page
	}

	@Test
	public void dirtyCleanTest() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		BTree tree = getTestTree(bufferManager);
		PagedBTreeNode root = (PagedBTreeNode) tree.getRoot();
		assertTrue(root.isDirty());
		bufferManager.write((PagedBTreeNode) tree.getRoot());
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

		// TODO: test number of writes

	}

	private PagedBTreeNode getTestLeaf(BTreeBufferManager bufferManager) {
		int order = 3;
		PagedBTreeNode leafNode = new PagedBTreeNode(bufferManager, order,
				true, true);
		leafNode.put(1, 2);
		return leafNode;
	}

	private PagedBTreeNode getTestInnerNode(BTreeBufferManager bufferManager,
			int order) {
		PagedBTreeNode innerNode = new PagedBTreeNode(bufferManager, order,
				false, true);
		return innerNode;
	}

	private BTree getTestTree(BTreeBufferManager bufferManager) {
		int order = 5;
		BTreeNodeFactory nodeFactory = new PagedBTreeNodeFactory(bufferManager);
		BTreeFactory factory = new BTreeFactory(order, nodeFactory);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		BTree tree = factory.getTree();
		return tree;
	}

}
