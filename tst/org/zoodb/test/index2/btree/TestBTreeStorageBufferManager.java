package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;
import org.zoodb.tools.ZooConfig;

public class TestBTreeStorageBufferManager {

	StorageRootInMemory storage;
	BTreeStorageBufferManager bufferManager;

	@Before
	public void resetStorage() {
		this.storage = new StorageRootInMemory(ZooConfig.getFilePageSize());
		boolean isUnique = true;
		this.bufferManager = new BTreeStorageBufferManager(storage, isUnique);
	}

	@Test
	public void testSaveLeaf() {
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertEquals(bufferManager.getDirtyBuffer().get(leafNode.getPageId()),
				leafNode);

		BTree tree = TestBTree.getTestTree(bufferManager);
		BTreeIterator it = new BTreeIterator(tree);
		while (it.hasNext()) {
			PagedBTreeNode node = (PagedBTreeNode) it.next();
			assertEquals(bufferManager.getDirtyBuffer().get(node.getPageId()),
					node);
		}
	}

	@Test
	public void testComputeOrder() {
		int pageSize = 128;
		assertEquals(8, BTreeStorageBufferManager.computeLeafOrder(pageSize));
		assertEquals(10,
				BTreeStorageBufferManager.computeInnerNodeOrder(pageSize));
	}

	@Test
	public void testWriteLeaf() {
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertEquals(0, storage.statsGetPageCount());
		int pageId = bufferManager.write(leafNode);

		assertEquals(2, storage.statsGetPageCount());
		assertEquals(leafNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage, true);
		assertEquals(leafNode, bufferManager2.read(pageId));
		assertFalse(bufferManager2.read(pageId).isDirty());
	}

	@Test
	public void testWriteInnerNode() {
		int order = bufferManager.getInnerNodeOrder();

		PagedBTreeNode innerNode = getTestInnerNode(bufferManager, order);
		int pageId = bufferManager.write(innerNode);

		assertEquals(2, storage.statsGetPageCount());
		assertEquals(innerNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage, true);
		assertEquals(innerNode, bufferManager2.read(pageId));
		assertFalse(bufferManager2.read(pageId).isDirty());
	}

	@Test
	public void testWriteTree() {
		BTree tree = getTestTree(bufferManager);
		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot());

		assertEquals(10, storage.statsGetPageCount());
		assertEquals(tree.getRoot(), bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage, true);
		PagedBTreeNode expectedRoot = bufferManager2.read(pageId);
		assertEquals(tree.getRoot(), expectedRoot);
		assertFalse(expectedRoot.isDirty());
	}

	@Test
	public void testDelete() {
		assertEquals(0, storage.statsGetPageCount());
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		int pageId = bufferManager.write(leafNode);
		assertEquals(2, storage.statsGetPageCount());

		bufferManager.remove(pageId);
		assertEquals(2, storage.statsGetPageCount());
		// System.out.println(storage.getFsm().debugPageIds());
		// assertTrue(storage.getFsm().debugIsPageIdInFreeList(pageId));
		// assertEquals(null, bufferManager.getMemoryBuffer().get(pageId));
	}

	@Test
	public void bufferLocationTest() {
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		bufferManager.write(leafNode);
		assertTrue(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		leafNode.put(2, 3);
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		int pageId = bufferManager.write(leafNode);

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage, true);
		PagedBTreeNode leafNode2 = bufferManager2.read(pageId);
		assertTrue(bufferManager2.getCleanBuffer().containsKey(
				leafNode2.getPageId()));
		assertFalse(bufferManager2.getDirtyBuffer().containsKey(
				leafNode2.getPageId()));

		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		leafNode = getTestLeaf(bufferManager);
		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		leafNode = getTestLeaf(bufferManager);
		bufferManager.write(leafNode);
		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));
	}

	@Test
	public void numWritesTest() {
		int expectedNumWrites = 0;
		UniquePagedBTree tree = (UniquePagedBTree) TestBTree
				.getTestTree(bufferManager);
		PagedBTreeNode root = tree.getRoot();
		assertEquals(expectedNumWrites, bufferManager.getStatNWrittenPages());

		bufferManager.write(tree.getRoot());
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 9,
				bufferManager.getStatNWrittenPages());

		tree.insert(4, 4);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 3,
				bufferManager.getStatNWrittenPages());

		tree.insert(32, 32);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 4,
				bufferManager.getStatNWrittenPages());

		tree.delete(16);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 4,
				bufferManager.getStatNWrittenPages());

		tree.delete(14);
		bufferManager.write(root);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 4,
				bufferManager.getStatNWrittenPages());
	}

	/*
	 * test whether multiple BufferManager can make use of the same storage
	 */
	@Test
	public void testInsertDeleteMassivelyWithDifferentOrder() {
		int innerOrder = bufferManager.getInnerNodeOrder();
		int leafOrder = bufferManager.getLeafOrder();

		BTreeFactory factory = new BTreeFactory(innerOrder, leafOrder,
				bufferManager, true);
		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage, true);
		UniquePagedBTree tree2 = new UniquePagedBTree(innerOrder, leafOrder,
				bufferManager2);

		insertDeleteMassively(factory, tree2, bufferManager2);

		this.resetStorage();
		UniquePagedBTree tree = new UniquePagedBTree(innerOrder, leafOrder,
				bufferManager);
		bufferManager2 = new BTreeStorageBufferManager(storage, true);
		tree2 = new UniquePagedBTree(innerOrder, leafOrder, bufferManager2);
		insertDeleteMassively2(tree, tree2, bufferManager2);
	}

	public void insertDeleteMassively(BTreeFactory factory,
			UniquePagedBTree tree2, BTreeStorageBufferManager bufferManager2) {
		int numEntries = 10000;
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries,
				42);

		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		tree.write();
		tree2.setRoot(constructRootWithNewBufferManager(tree.getRoot(),bufferManager2));

		// check whether all entries are inserted
		for (LLEntry entry : entries) {
			assertEquals(new Long(entry.getValue()), tree2.search(entry.getKey()));
		}

		assertEquals(0, bufferManager2.getDirtyBuffer().size());

		// delete every entry and check that there is indeed no entry anymore
		for (LLEntry entry : entries) {
			tree.delete(entry.getKey());
		}

		tree.write();
		tree2.setRoot(constructRootWithNewBufferManager(tree.getRoot(),bufferManager2));

		for (LLEntry entry : entries) {
			assertEquals(null, tree2.search(entry.getKey()));
		}

		// root is empty and has no children
		assertEquals(0, tree2.getRoot().getNumKeys());
		BTreeNode[] emptyChildren = new BTreeNode[tree2.getRoot().getOrder()];
		Arrays.fill(emptyChildren, null);
		assertArrayEquals(emptyChildren, tree2.getRoot().getChildren());
	}

	public void insertDeleteMassively2(UniquePagedBTree tree,
			UniquePagedBTree tree2, BTreeStorageBufferManager bufferManager2) {
		int numEntries = 10000;
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries,
				42);

		// add all entries, delete a portion of it, check that correct ones are
		// deleted and still present respectively
		int split = (9 * numEntries) / (10 * numEntries);
		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}
		int i = 0;
		for (LLEntry entry : entries) {
			if (i < split) {
				tree.delete(entry.getKey());
			} else {
				break;
			}
			i++;
		}

		tree.write();
		tree2.setRoot(constructRootWithNewBufferManager(tree.getRoot(),bufferManager2));

		i = 0;
		for (LLEntry entry : entries) {
			if (i < split) {
				assertEquals(null, tree2.search(entry.getKey()));
			} else {
				assertEquals(new Long(entry.getValue()), tree2.search(entry.getKey()));
			}
			i++;
		}

		// there is only one root
		BTreeIterator it = new BTreeIterator(tree2);
		assertTrue(it.next().isRoot());
		while (it.hasNext()) {
			assertFalse(it.next().isRoot());
		}
	}

	private PagedBTreeNode getTestLeaf(BTreeStorageBufferManager bufferManager) {
		int order = bufferManager.getLeafOrder();
		PagedBTreeNode leafNode = new UniquePagedBTreeNode(bufferManager,
				order, true, true);
		leafNode.put(1, 2);
		return leafNode;
	}

	private PagedBTreeNode getTestInnerNode(BTreeBufferManager bufferManager,
			int order) {
		PagedBTreeNode innerNode = new UniquePagedBTreeNode(bufferManager,
				order, false, true);
		return innerNode;
	}

	public static PagedBTree getTestTree(
			BTreeStorageBufferManager bufferManager) {
		BTreeFactory factory = new BTreeFactory(
				bufferManager.getInnerNodeOrder(),
				bufferManager.getLeafOrder(), bufferManager, true);
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

	PagedBTreeNode constructRootWithNewBufferManager(PagedBTreeNode prevRoot,
			BTreeBufferManager bufferManager) {
		if (!prevRoot.isLeaf()) {
			return PagedBTreeNodeFactory.constructInnerNode(bufferManager, true, true,
					prevRoot.getOrder(), prevRoot.getPageId(),
					prevRoot.getNumKeys(), prevRoot.getKeys(),
					prevRoot.getValues(), prevRoot.getChildrenPageIds());
		} else {
			return PagedBTreeNodeFactory.constructLeaf(bufferManager, true, true,
					prevRoot.getOrder(), prevRoot.getPageId(),
					prevRoot.getNumKeys(), prevRoot.getKeys(),
					prevRoot.getValues());

		}

	}
}
