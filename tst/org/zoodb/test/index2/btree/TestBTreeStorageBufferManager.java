/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 *
 * This file is part of ZooDB.
 *
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 *
 * See the README and COPYING files for further information.
 */
package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;
import org.zoodb.tools.ZooConfig;

public class TestBTreeStorageBufferManager {

	private IOResourceProvider io;
	private StorageChannelOutput out;
	StorageRootInMemory storage;
	BTreeStorageBufferManager bufferManager;
    int pageSize;

	@Before
	public void resetStorage() {
		this.storage = new StorageRootInMemory(ZooConfig.getFilePageSize());
		this.io = storage.createChannel();
		this.out = io.createWriter(false);
        this.pageSize = storage.getPageSize();
		boolean isUnique = true;
		this.bufferManager = new BTreeStorageBufferManager(storage.createChannel(), isUnique);
	}

	@Test
	public void testSaveLeaf() {
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertEquals(bufferManager.getDirtyBuffer().get(leafNode.getPageId()),
				leafNode);

		BTree tree = getTestTreeWithThreeLayers(bufferManager);
		BTreeIterator it = new BTreeIterator(tree);
		while (it.hasNext()) {
			PagedBTreeNode node = (PagedBTreeNode) it.next();
			assertEquals(bufferManager.getDirtyBuffer().get(node.getPageId()),
					node);
		}
	}

    public PagedBTreeNode testComputeSizeHelper(int pageSize) {
	    long[] keys = new long[] {-2139342741, -2139288335, -2139258968, -2139174228, -2139057184, -2139020661,
                -2138955401, -2138947348, -2138906006, -2138854958, -2138810679, -2138737967,
                -2138713939, -2138705771, -2138702613, -2138681828, -2138638271, -2138599336,
                -2138560423, -2138384792};

        long[] values = new long[] {1451443246,2045976137,1497430374,1772670428,-694016354,-646761731,
                -688167221, 263241308, 780826785, 1819848219, -1429631675, -810669383, -1208608663,
                1954442242, 386809692, -984072888, -54873685, 888823278, -671249934, -736916506};

        IOResourceProvider currentStorage = new StorageRootInMemory(pageSize).createChannel();
        BTreeBufferManager currentBufferManager = new BTreeStorageBufferManager(currentStorage, false);
        PagedBTreeNode node = new NonUniquePagedBTreeNode(currentBufferManager, pageSize, true, false);

        for (int i = 0; i < keys.length; i++) {
            node.put(keys[i], values[i], false);
        }
        
        return node;
    }
    
    @Test
    public void testComputeSize() {
        int pageSize = 244;
        
        PagedBTreeNode node = testComputeSizeHelper(pageSize);
        assertTrue("We want to test here if it exactly fits into the page", node.computeSize() == pageSize);
        try {
	        node.getBufferManager().write((PagedBTreeNode) node, out);
        } catch(BufferOverflowException e) {
        	fail("Too many elements in Node.");
        }
        
        pageSize = 243;
        node = testComputeSizeHelper(pageSize);
        assertTrue("We want to test here if it does not fit into the page", node.computeSize() == pageSize+1);
        try {
	        node.getBufferManager().write((PagedBTreeNode) node, io.createWriter(false));
	        fail("Putting too many elements in node should result in exception.");
        } catch(BufferOverflowException e) {
        	// empty
        }
    }

	@Test
	public void testWriteLeaf() {
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertEquals(0, storage.statsGetPageCount());
		int pageId = bufferManager.write(leafNode, out);

		assertEquals(2, storage.statsGetPageCount());
		bufferManager.getCleanBuffer().clear();
		assertEquals(leafNode, bufferManager.read(pageId));

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage.createChannel(), true);
		PagedBTreeNode readLeafNode = bufferManager2.read(pageId);
		assertEquals(leafNode, readLeafNode);
		assertFalse(bufferManager2.read(pageId).isDirty());
	}

	@Test
	public void testWriteEmptyNodes() {
		/* Leaf */
		PagedBTreeNode leaf = getTestEmptyLeaf(bufferManager);
		int pageId = bufferManager.write(leaf, out);

		assertEquals(2, storage.statsGetPageCount());
		assertEquals(leaf, bufferManager.read(pageId));

		bufferManager.clear(leaf);
		assertEquals(leaf, bufferManager.read(pageId));
		assertFalse(bufferManager.read(pageId).isDirty());

		/* Inner node */
		PagedBTreeNode inner_node = getTestEmptyLeaf(bufferManager);
		pageId = bufferManager.write(inner_node, out);

		assertEquals(3, storage.statsGetPageCount());
		assertEquals(inner_node, bufferManager.read(pageId));

		bufferManager.clear(inner_node);
		assertEquals(inner_node, bufferManager.read(pageId));
		assertFalse(bufferManager.read(pageId).isDirty());
	}

	@Test
	public void testWriteSmallTree() {
		BTree tree = getTestTreeWithTwoLayers(bufferManager);
		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot(), out);

		assertEquals(4, storage.statsGetPageCount());
		assertEquals(tree.getRoot(), bufferManager.read(pageId));
		
		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage.createChannel(), true);
		PagedBTreeNode expectedRoot = bufferManager2.read(pageId);
		assertEquals(tree.getRoot(), expectedRoot);
		assertFalse(expectedRoot.isDirty());
	}

	@Test
	public void testWriteTree() {
		BTree tree = getTestTreeWithThreeLayers(bufferManager);
		int pageId = bufferManager.write((PagedBTreeNode) tree.getRoot(), out);

		assertEquals(10, storage.statsGetPageCount());
		assertEquals(tree.getRoot(), bufferManager.read(pageId));
		
		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage.createChannel(), true);
		PagedBTreeNode expectedRoot = bufferManager2.read(pageId);
		assertEquals(tree.getRoot(), expectedRoot);
		assertFalse(expectedRoot.isDirty());
	}

	@Test
	public void testDelete() {
		assertEquals(0, storage.statsGetPageCount());
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		int pageId = bufferManager.write(leafNode, out);
		assertEquals(2, storage.statsGetPageCount());

		bufferManager.remove(bufferManager.read(pageId));
		assertEquals(2, storage.statsGetPageCount());
		assertTrue(storage.debugIsPageIdInFreeList(pageId));
	}
	
	@Test
	public void testClose() {
		int numEntries = 10000;
		UniquePagedBTree tree = new UniquePagedBTree(bufferManager.getPageSize(), bufferManager);
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries,
				42);

		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}
		// collect all page ids
		List<Integer> pageIds = new ArrayList<Integer>();
		BTreeIterator it = new BTreeIterator(tree);
		while(it.hasNext()) {
			int pageId = ((PagedBTreeNode)it.next()).getPageId();
			pageIds.add(pageId);
			assertTrue(bufferManager.getDirtyBuffer().containsKey(pageId));
		}
		assertEquals(0,bufferManager.getCleanBuffer().size());
		assertEquals(pageIds.size(),bufferManager.getDirtyBuffer().size());
		tree.write(out);
		
		// collect all page ids
		pageIds = new ArrayList<Integer>();
		it = new BTreeIterator(tree);
		while(it.hasNext()) {
			int pageId = ((PagedBTreeNode)it.next()).getPageId();
			pageIds.add(pageId);
		}

		for (LLEntry entry : entries) {
			tree.delete(entry.getKey());
		}
		
		assertEquals(0,bufferManager.getCleanBuffer().size());
		assertEquals(1,bufferManager.getDirtyBuffer().size());
		int rootPageId = tree.getRoot().getPageId();
		assertFalse(storage.debugIsPageIdInFreeList(rootPageId));
		pageIds.remove(rootPageId);
		// check whether pages are freed
		for(Integer pageId : pageIds) {
			assertTrue(storage.debugIsPageIdInFreeList(pageId));
		}
		
		tree.write(out);
		rootPageId = tree.getRoot().getPageId();
		assertFalse(storage.debugIsPageIdInFreeList(rootPageId));
		assertEquals(1,bufferManager.getCleanBuffer().size());
		assertEquals(0,bufferManager.getDirtyBuffer().size());
	}

	@Test
	public void bufferLocationTest() {
		PagedBTreeNode leafNode = getTestLeaf(bufferManager);
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		bufferManager.write(leafNode, out);
		assertTrue(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		leafNode.put(2, 3, false);
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));

		int pageId = bufferManager.write(leafNode, out);

		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage.createChannel(), true);
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
		bufferManager.write(leafNode, out);
		leafNode.close();
		assertFalse(bufferManager.getCleanBuffer().containsKey(
				leafNode.getPageId()));
		assertFalse(bufferManager.getDirtyBuffer().containsKey(
				leafNode.getPageId()));
		
	}
	
	@Test
	public void bufferLocationTestTree() {
		UniquePagedBTree tree = (UniquePagedBTree) TestBTree
				.getTestTreeWithThreeLayers(bufferManager);
		bufferManager.write(tree.getRoot(), out);
		
		tree.delete(8);

		assertEquals(2, bufferManager.getDirtyBuffer().size());
		PagedBTreeNode root = tree.getRoot();
		assertTrue(bufferManager.getDirtyBuffer().containsKey(root.getPageId()));
		assertTrue(bufferManager.getDirtyBuffer().containsKey(((PagedBTreeNode)root.getChild(0)).getPageId()));
		bufferManager.write(tree.getRoot(), out);
		
		System.out.println(tree);
		tree.delete(3);
		System.out.println(tree);
		assertTrue(bufferManager.getDirtyBuffer().containsKey(root.getPageId()));
		assertFalse(bufferManager.getCleanBuffer().containsKey(root.getPageId()));
		bufferManager.write(tree.getRoot(), out);
		assertFalse(bufferManager.getDirtyBuffer().containsKey(root.getPageId()));
		assertTrue(bufferManager.getCleanBuffer().containsKey(root.getPageId()));
	}
	
    @Test
	public void markDirtyTest() {
		int pageSize = 64;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(new StorageRootInMemory(pageSize).createChannel(), true);
		UniquePagedBTree tree = (UniquePagedBTree) getTestTreeWithThreeLayers(bufferManager);
		PagedBTreeNode root = tree.getRoot();
		assertTrue(root.isDirty());
		bufferManager.write( tree.getRoot(), out);
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
//		assertFalse(lvl2child2.isDirty());
		assertFalse(lvl2child3.isDirty());
		assertFalse(lvl1child2.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());
		assertFalse(lvl2child6.isDirty());

		bufferManager.write(tree.getRoot(), out);

		tree.insert(32, 32);
		tree.insert(35, 35);
		tree.insert(36, 36);
		PagedBTreeNode lvl2child7 = (PagedBTreeNode) lvl1child2.getChild(3);
		assertTrue(root.isDirty());
		assertTrue(lvl1child2.isDirty());
		assertTrue(lvl2child6.isDirty());
		assertTrue(lvl2child7.isDirty());
		assertFalse(lvl1child1.isDirty());
		assertFalse(lvl2child1.isDirty());
		assertFalse(lvl2child2.isDirty());
		assertFalse(lvl2child3.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());

		bufferManager.write(tree.getRoot(), out);
		tree.delete(16);
		assertTrue(root.isDirty());
		assertTrue(lvl1child1.isDirty());
//		assertTrue(lvl1child2.isDirty());
		assertFalse(lvl2child1.isDirty());
//		assertTrue(lvl2child2.isDirty());
//		assertTrue(lvl2child3.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());
		assertFalse(lvl2child6.isDirty());
		assertFalse(lvl2child7.isDirty());

		bufferManager.write(tree.getRoot(), out);
		tree.delete(14);
		// root does not exist anymore
		// lvl1child1 does not exist anymore
//		assertTrue(lvl1child2.isDirty());
		// lvl2child1 does not exist anymore
		// lvl2child2 does not exist anymore
//		assertTrue(lvl2child3.isDirty());
		assertFalse(lvl2child4.isDirty());
		assertFalse(lvl2child5.isDirty());
		assertFalse(lvl2child6.isDirty());
		assertFalse(lvl2child7.isDirty());
	}

	@Test
	public void closeTest() {
		UniquePagedBTree tree = (UniquePagedBTree) getTestTreeWithThreeLayers(bufferManager);

		// build list of initial nodes
		ArrayList<Integer> nodeList = new ArrayList<Integer>();
		BTreeIterator iterator = new BTreeIterator(tree);
		while (iterator.hasNext()) {
			nodeList.add(((PagedBTreeNode) iterator.next()).getPageId());
		}

		tree.delete(2);
		tree.delete(3);
		closeTestHelper(tree, nodeList, bufferManager);

		tree.delete(5);
		tree.delete(7);
		tree.delete(8);
		closeTestHelper(tree, nodeList, bufferManager);

        tree.delete(24);
		tree.delete(27);
		tree.delete(29);
		tree.delete(33);
		closeTestHelper(tree, nodeList, bufferManager);

		tree.delete(14);
		tree.delete(16);
		closeTestHelper(tree, nodeList, bufferManager);

		tree.delete(19);
		tree.delete(20);
		tree.delete(22);
		closeTestHelper(tree, nodeList, bufferManager);
	}
	
	// test whether all of the nodes that are not in the tree anymore are also
	// not anymore present in the BufferManager
	private void closeTestHelper(UniquePagedBTree tree,
			ArrayList<Integer> nodeList, BTreeStorageBufferManager bufferManager) {

		ArrayList<Integer> removedNodeList = new ArrayList<Integer>(nodeList);
		
		BTreeIterator iterator = new BTreeIterator(tree);
		while (iterator.hasNext()) {
			Integer nodeId = ((PagedBTreeNode) iterator.next()).getPageId();
			removedNodeList.remove(nodeId);
		}
		for (int nodeId : removedNodeList) {
			assertEquals(null, bufferManager.readNodeFromMemory(nodeId));
		}
	}

	@Test
	public void numWritesTest() {
		int expectedNumWrites = 0;
		UniquePagedBTree tree = (UniquePagedBTree) getTestTreeWithThreeLayers(bufferManager);
		PagedBTreeNode root = tree.getRoot();
		assertEquals(expectedNumWrites, bufferManager.getStatNWrittenPages());

		bufferManager.write(tree.getRoot(), out);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 9,
				bufferManager.getStatNWrittenPages());

		tree.insert(4, 4);
		bufferManager.write(root, out);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 3,
				bufferManager.getStatNWrittenPages());

		tree.insert(32, 32);
		bufferManager.write(root, out);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 3,
				bufferManager.getStatNWrittenPages());

		tree.delete(16);
		bufferManager.write(root, out);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 3,
				bufferManager.getStatNWrittenPages());

		tree.delete(14);
		bufferManager.write(tree.getRoot(), out);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		assertEquals(expectedNumWrites += 2,
				bufferManager.getStatNWrittenPages());
	}

	/*
	 * test whether multiple BufferManager can make use of the same storage
	 */
	@Test
	public void testInsertDeleteMassively() {
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
		BTreeStorageBufferManager bufferManager2 = new BTreeStorageBufferManager(
				storage.createChannel(), true);

		insertDeleteMassively(factory, bufferManager2);

		this.resetStorage();
		UniquePagedBTree tree = new UniquePagedBTree(pageSize, bufferManager);
		bufferManager2 = new BTreeStorageBufferManager(storage.createChannel(), true);
		insertDeleteMassively2(tree, bufferManager2);
	}

	public void insertDeleteMassively(BTreeFactory factory,
			BTreeStorageBufferManager bufferManager2) {
		int numEntries = 10000;
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries,
				42);

		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}

		tree.write(out);
		assertEquals(0, bufferManager2.getDirtyBuffer().size());
		
		UniquePagedBTree tree2 = new UniquePagedBTree((UniquePagedBTreeNode)tree.getRoot(), tree.getPageSize(),
				bufferManager2);

		// check whether all entries are inserted
		for (LLEntry entry : entries) {
			assertEquals(Long.valueOf(entry.getValue()), tree2.search(entry.getKey()));
		}

		assertEquals(0, bufferManager2.getDirtyBuffer().size());

		// delete every entry and check that there is indeed no entry anymore
		for (LLEntry entry : entries) {
			tree.delete(entry.getKey());
		}

		tree.write(out);
		tree2 = new UniquePagedBTree((UniquePagedBTreeNode)tree.getRoot(), tree.getPageSize(),
				bufferManager2);

		for (LLEntry entry : entries) {
			assertEquals(null, tree2.search(entry.getKey()));
		}

		// root is empty and has no children
		assertEquals(0, tree2.getRoot().getNumKeys());
		assertTrue(tree2.getRoot().isLeaf());
		assertArrayEquals(null, tree2.getRoot().getChildrenPageIds());
	}

	public void insertDeleteMassively2(UniquePagedBTree tree,
			BTreeStorageBufferManager bufferManager2) {
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

		tree.write(out);
		UniquePagedBTree tree2 = new UniquePagedBTree((UniquePagedBTreeNode)tree.getRoot(), tree.getPageSize(),
				bufferManager2);

		i = 0;
		for (LLEntry entry : entries) {
			if (i < split) {
				assertEquals(null, tree2.search(entry.getKey()));
			} else {
				assertEquals(Long.valueOf(entry.getValue()), tree2.search(entry.getKey()));
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
	
	@Test
	public void testNonUnique() {
        final int MAX = 1000;
        NonUniquePagedBTree tree = new NonUniquePagedBTree(pageSize, bufferManager);

        //Fill index
        for (int j = 0; j < 10; j++) {
	        for (int i = 1000; i < 1000+MAX; i++) {
	            tree.insert(i, 32+i*j);
	        }
        }

        tree.write(out);
        
        bufferManager.getCleanBuffer().clear();
        for (int j = 0; j < 10; j++) {
	        for (int i = 1000; i < 1000+MAX; i++) {
	            assertTrue(tree.contains(i, 32+i*j));
	        }
        }
	}
	
	@Test
	public void testCache() {
		int numEntries = 10000;
		bufferManager.setMaxCleanBufferElements(10);
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		List<LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries,
				42);

		for (LLEntry entry : entries) {
			tree.insert(entry.getKey(), entry.getValue());
		}
		tree.write(out);
		
		// check if all entries are inserted
		for (LLEntry entry : entries) {
			assertEquals(Long.valueOf(entry.getValue()), tree.search(entry.getKey()));
		}
		
		System.out.println(bufferManager.getCleanBuffer().size());
		assertTrue(bufferManager.getCleanBuffer().size() <= 1000);
		assertEquals(0, bufferManager.getDirtyBuffer().size());
		
		List<Integer> pageIds = getPageIds(tree); 
		bufferManager.clear(tree.getRoot());
		
		// check if all pageIds are freed
		for(Integer pageId : pageIds) {
			assertTrue(storage.debugIsPageIdInFreeList(pageId));
		}
	}
	
    private PagedBTreeNode getTestEmptyLeaf(BTreeStorageBufferManager bufferManager) {
		PagedBTreeNode leaf = new UniquePagedBTreeNode(bufferManager,
				bufferManager.getPageSize(), true, true);
		return leaf;
	}
	
	private PagedBTreeNode getTestLeaf(BTreeStorageBufferManager bufferManager) {
		PagedBTreeNode leafNode = new UniquePagedBTreeNode(bufferManager,
				bufferManager.getPageSize(), true, true);
		leafNode.put(1, 2, false);
		return leafNode;
	}

    public static PagedBTree getTestTreeWithTwoLayers(
			BTreeStorageBufferManager bufferManager) {
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(5L), Arrays.asList(13L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();

		return tree;
	}

	public static PagedBTree getTestTreeWithThreeLayers(
			BTreeStorageBufferManager bufferManager) {
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
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
					prevRoot.getPageSize(), prevRoot.getPageId(),
					prevRoot.getNumKeys(), prevRoot.getKeys(),
					prevRoot.getValues(), prevRoot.getChildrenPageIds());
		} else {
			return PagedBTreeNodeFactory.constructLeaf(bufferManager, true, true,
					prevRoot.getPageSize(), prevRoot.getPageId(),
					prevRoot.getNumKeys(), prevRoot.getKeys(),
					prevRoot.getValues());

		}
	}
	
	public List<Integer> getPageIds(PagedBTree tree) {
		List<Integer> pageIds = new ArrayList<Integer>();
		BTreeIterator it = new BTreeIterator(tree);
		while(it.hasNext()) {
			int pageId = ((PagedBTreeNode)it.next()).getPageId();
			pageIds.add(pageId);
		}
		return pageIds;
	}
	
}
