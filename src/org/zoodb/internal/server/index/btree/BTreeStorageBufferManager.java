package org.zoodb.internal.server.index.btree;

import java.util.HashMap;
import java.util.Map;

public class BTreeStorageBufferManager implements BTreeBufferManager {
	private Map<Integer, PagedBTreeNode> map;
	private int pageIdCounter;

	public BTreeStorageBufferManager() {
		this.map = new HashMap<Integer, PagedBTreeNode>();
		this.pageIdCounter = 0;
	}

	@Override
	public PagedBTreeNode read(int pageId) {
		// search node in memory
		PagedBTreeNode node = map.get(pageId);
		if (node != null) {
			return node;
		}

		// search node in storage
		return readNodeFromStorage(pageId);
	}

	public PagedBTreeNode readNodeFromStorage(int pageId) {
		// TODO: 
		return null;
	}

	@Override
	public int write(PagedBTreeNode node) {
		if (!node.isDirty()) {
			return node.getPageId();
		}
		if (!node.isLeaf()) /* is inner node */{
			// write children
			int childIndex = 0;
			for (int childPageId : node.getChildrenPageIdList()) {
				// TODO: optimize: if children is not in memory, then it can not
				// be dirty
				PagedBTreeNode child = read(childPageId);
				int newChildPageId = write(child);
				node.setChildPageId(childIndex, childPageId);

				childIndex++;
			}
		}
		// write data to storage and obtain new pageId
		int newPageId = writeNodeDataToStorage(node);

		// update pageId in memory
		map.remove(node.getPageId());
		// TODO: handle case when newPageId is already in map for another node
		// n'.
		// This could happen if n' was never written to the storage before, so
		// it received a pageId which is not associated with a page.
		map.put(newPageId, node);

		node.setPageId(newPageId);

		node.markClean();
		return newPageId;
	}

	int writeNodeDataToStorage(PagedBTreeNode node) {
		// TODO:
		return 0;
	}

	@Override
	public int save(PagedBTreeNode node) {
		pageIdCounter++;
		// TODO: handle case that this key is already reserved for the "real" id
		// of a page.
		map.put(pageIdCounter, node);

		return pageIdCounter;
	}

	@Override
	public void delete(int id) {
		map.remove(id);
		// TODO: call delete
		// TODO: free page in storage

	}
}
