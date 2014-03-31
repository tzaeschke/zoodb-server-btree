package org.zoodb.test.index2.btree;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.*;
import org.zoodb.tools.ZooConfig;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TestBTreeStorageBufferManager {

	StorageChannel storage;

	@Before 
	public void resetStorage() {
		this.storage = new StorageRootInMemory(
			ZooConfig.getFilePageSize());
	}
	@Test
	public void testWriteLeaf() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertEquals(0, storage.statsGetPageCount());
		int pageId = bufferManager.write(leafNode);

		assertEquals(2, storage.statsGetPageCount());
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

		assertEquals(2, storage.statsGetPageCount());
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

		BTree tree = TestBTree.getTestTree(bufferManager);
		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot());

		assertEquals(10, storage.statsGetPageCount());
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
		assertEquals(2, storage.statsGetPageCount());

		bufferManager.delete(pageId);
		assertEquals(null, bufferManager.getMemoryBuffer().get(pageId));
		assertEquals(0, storage.statsGetPageCount());
		// assertEquals(null, bufferManager.read(pageId));
		// does not work because data is still on the page
	}

	@Test
	public void dirtyCleanTest() {
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



}
