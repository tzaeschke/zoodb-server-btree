package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.*;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;
import org.zoodb.tools.ZooConfig;

public class TestBTreeStorageBufferManager {

	StorageChannel storage;

	@Before 
	public void resetStorage() {
		this.storage = new StorageRootInMemory(
			ZooConfig.getFilePageSize());
	}
	
	@Test
	public void testSaveLeaf() {
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertEquals(bufferManager.getDirtyBuffer().get(leafNode.getPageId()), leafNode);
		
		BTree tree = TestBTree.getTestTree(bufferManager);
		BTreeIterator it = new BTreeIterator(tree);
		while(it.hasNext()) {
			PagedBTreeNode node = (PagedBTreeNode) it.next();
			assertEquals(bufferManager.getDirtyBuffer().get(node.getPageId()),node);
		}
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
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);
		int order = bufferManager.computeOrder();

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

		BTree tree = getTestTree(bufferManager);
		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot());

		assertEquals(10, storage.statsGetPageCount());
		assertEquals(tree.getRoot(), bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage);
		PagedBTreeNode expectedRoot = bufferManager2.read(pageId);
		assertEquals(tree.getRoot(), expectedRoot);
		assertFalse(expectedRoot.isDirty());
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
		// assertEquals(0, storage.statsGetPageCount());
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

		tree.delete(14);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites+=4, bufferManager.getStatNWrittenPages());
	}

	private PagedBTreeNode getTestLeaf(BTreeStorageBufferManager bufferManager) {
		int order = bufferManager.computeOrder();
		PagedBTreeNode leafNode = new UniquePagedBTreeNode(bufferManager, order, true, true);
		leafNode.put(1, 2);
		return leafNode;
	}

	private PagedBTreeNode getTestInnerNode(BTreeBufferManager bufferManager,int order) {
		PagedBTreeNode innerNode = new UniquePagedBTreeNode(bufferManager, order,false, true);
		return innerNode;
	}
	
	public static BTree<PagedBTreeNode> getTestTree(BTreeStorageBufferManager bufferManager) {
		int order = bufferManager.computeOrder();
		BTreeFactory factory = new BTreeFactory(order, bufferManager, true);
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

}
