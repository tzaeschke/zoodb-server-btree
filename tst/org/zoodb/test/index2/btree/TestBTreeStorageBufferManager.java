package org.zoodb.test.index2.btree;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.unique.UniqueBTreeUtils;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;
import org.zoodb.tools.ZooConfig;

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

		bufferManager.remove(pageId);
		assertEquals(null, bufferManager.getMemoryBuffer().get(pageId));
		assertEquals(0, storage.statsGetPageCount());
		// assertEquals(null, bufferManager.read(pageId));
		// does not work because data is still on the page
	}
	
	@Test
	public void bufferLocationTest() {
        BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertFalse(bufferManager.getCleanBuffer().containsKey(leafNode.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(leafNode.getPageId()));
		
		bufferManager.write(leafNode);
		assertTrue(bufferManager.getCleanBuffer().containsKey(leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(leafNode.getPageId()));
		
		leafNode.put(2, 3);
		assertFalse(bufferManager.getCleanBuffer().containsKey(leafNode.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(leafNode.getPageId()));

		int pageId = bufferManager.write(leafNode);
		
        BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		PagedBTreeNode leafNode2 = bufferManager2.read(pageId);
		assertTrue(bufferManager2.getCleanBuffer().containsKey(leafNode2.getPageId()));
		assertFalse(bufferManager2.getDirtyBuffer().containsKey(leafNode2.getPageId()));
		
		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(leafNode.getPageId()));
		
		leafNode = getTestLeaf(bufferManager);
		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(leafNode.getPageId()));
		
        leafNode = getTestLeaf(bufferManager);
        bufferManager.write(leafNode);
		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(leafNode.getPageId()));
	}

	@Test
	public void numWritesTest() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		int expectedNumWrites = 0;
		UniquePagedBTree tree = (UniquePagedBTree) TestBTree.getTestTree(bufferManager);
		PagedBTreeNode root = tree.getRoot();
		assertEquals(expectedNumWrites, bufferManager.getStatNWrittenPages());

		bufferManager.write(tree.getRoot());
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites+=9, bufferManager.getStatNWrittenPages());

		tree.insert(4, 4);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites+=3, bufferManager.getStatNWrittenPages());

		tree.insert(32, 32);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites+=4, bufferManager.getStatNWrittenPages());

		tree.delete(16);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites+=4, bufferManager.getStatNWrittenPages());

		System.out.println(tree);
		tree.delete(14);
		System.out.println(tree);
		bufferManager.write(root);
		System.out.println(bufferManager.getDirtyBuffer());
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites+=5, bufferManager.getStatNWrittenPages());
//		assertTrue(root.isDirty());
//		assertTrue(lvl1child1.isDirty());
//		assertTrue(lvl1child2.isDirty());
//		assertFalse(lvl2child1.isDirty());
//		assertTrue(lvl2child2.isDirty());
//		assertTrue(lvl2child3.isDirty());
//		assertFalse(lvl2child4.isDirty());
//		assertFalse(lvl2child5.isDirty());
//		assertFalse(lvl2child6.isDirty());
//		assertFalse(lvl2child7.isDirty());
	}

	private PagedBTreeNode getTestLeaf(BTreeBufferManager bufferManager) {
		int order = 3;
		PagedBTreeNode leafNode = new UniquePagedBTreeNode(bufferManager, order, true, true);
		UniqueBTreeUtils.put(leafNode, 1, 2);
		return leafNode;
	}

	private PagedBTreeNode getTestInnerNode(BTreeBufferManager bufferManager,int order) {
		PagedBTreeNode innerNode = new UniquePagedBTreeNode(bufferManager, order,false, true);
		return innerNode;
	}

}
