package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.tools.ZooConfig;

public class TestBTreeStorageBufferManager {

	@Test
	public void testIO() {
		int order = 3;
		StorageChannel storage = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(
				storage);

		PagedBTreeNode leafNode = new PagedBTreeNode(bufferManager, null,
				order, true);
		leafNode.put(1, 2);
		int pageId = bufferManager.write(leafNode);
		assertEquals(leafNode, bufferManager.read(pageId));
		
        PagedBTreeNode innerNode = new PagedBTreeNode(bufferManager, null,
				order, false);
		pageId = bufferManager.write(innerNode);
		assertEquals(innerNode, bufferManager.read(pageId));
	}
}
