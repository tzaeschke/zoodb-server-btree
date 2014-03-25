package org.zoodb.internal.server.index.btree;

import java.util.HashMap;
import java.util.Map;

public class BTreeHashBufferManager implements BTreeBufferManager {
	private Map<Integer, PagedBTreeNode> map;
	private int pageId;

	public BTreeHashBufferManager() {
		this.map = new HashMap<Integer, PagedBTreeNode>();
		this.pageId = 0;
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
	public void delete(int id) {
		map.remove(id);
		return; 
	}

	@Override
	public void clear() {
		pageId = 0;
		map.clear();
	}

}
