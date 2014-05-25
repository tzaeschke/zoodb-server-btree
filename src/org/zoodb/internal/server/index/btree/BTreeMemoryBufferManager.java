package org.zoodb.internal.server.index.btree;

import java.util.HashMap;
import java.util.Map;

/*
 * BufferManager without using a proper storage for testing purposes.
 */
public class BTreeMemoryBufferManager implements BTreeBufferManager {
	private Map<Integer, PagedBTreeNode> map;
	private int pageId;
	private int pageSize;

	public BTreeMemoryBufferManager() {
		this.map = new HashMap<Integer, PagedBTreeNode>();
		this.pageId = 0;
		this.pageSize = 256;
	}
	
	public BTreeMemoryBufferManager(int pageSize) {
		this.map = new HashMap<Integer, PagedBTreeNode>();
		this.pageId = 0;
		this.pageSize = pageSize;
	}
	
	@Override
	public PagedBTreeNode read(int pageId) {
		return map.get(pageId);
	}

	@Override
	public int write(PagedBTreeNode node) {
		pageId++;
		map.put(pageId, node);
		return pageId;
	}

	@Override
	public int save(PagedBTreeNode node) {
		pageId++;
		map.put(pageId, node);
		return pageId;
	}

	@Override
	public void remove(int id) {
		map.remove(id);
		return; 
	}

	@Override
	public void clear(PagedBTreeNode node) {
		pageId = 0;
		map.clear();
	}

	@Override
	public int getPageSize() {
		return this.pageSize;
	}

	@Override
	public int getNodeSizeInStorage(PagedBTreeNode node) {
		int size = 0;
		size += BTreeStorageBufferManager.pageHeaderSize();
		size += node.getNonKeyEntrySizeInBytes() + node.getKeyArraySizeInBytes();
		
		return size;
	}

    @Override
    public int getNodeHeaderSizeInStorage(PagedBTreeNode node) {
        return BTreeStorageBufferManager.pageHeaderSize();
    }

	@Override
	public int getNodeValueElementSize() {
		return 8;
	}

	@Override
	public void updatePageStatus(PagedBTreeNode node) {
		// do nothing
	}
}
