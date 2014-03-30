package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTree;
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
		int order = 3;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode leafNode = new PagedBTreeNode(bufferManager, order,
				true, true);
		leafNode.put(1, 2);
		int pageId = bufferManager.write(leafNode);
		assertEquals(leafNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		assertEquals(leafNode, bufferManager2.read(pageId));
	}

	@Test
	public void testWriteInnerNode() {
		int order = 3;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);
		PagedBTreeNode innerNode = new PagedBTreeNode(bufferManager, order,
				false, true);
		int pageId = bufferManager.write(innerNode);
		assertEquals(innerNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		assertEquals(innerNode, bufferManager2.read(pageId));
	}

	@Test
	public void testWriteTree() {
		int order = 5;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);
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

		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot());
		assertEquals(tree.getRoot(), bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		assertEquals(tree.getRoot(), bufferManager2.read(pageId));
	}
}
