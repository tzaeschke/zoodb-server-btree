package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.tools.ZooConfig;

import static org.junit.Assert.assertEquals;

public class TestBTreeStorageBufferManager {

	@Test
	public void testIO() {
		int order = 3;
		StorageChannel storage = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode leafNode = new PagedBTreeNode(bufferManager, order, true, true);
		leafNode.put(1, 2);
		int pageId = bufferManager.write(leafNode);
		assertEquals(leafNode, bufferManager.read(pageId));
		
        PagedBTreeNode innerNode = new PagedBTreeNode(bufferManager, order, false, true);
		pageId = bufferManager.write(innerNode);
		assertEquals(innerNode, bufferManager.read(pageId));
	}
}
