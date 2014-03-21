package org.zoodb.internal.server.index.btree;

import java.util.HashMap;
import java.util.Map;

public class BTreeBufferManager {
	private Map<Integer, PagedBTreeNode> map;
	private int pageId;

	public BTreeBufferManager() {
		this.map = new HashMap<Integer, PagedBTreeNode>();
	}
	
	public PagedBTreeNode read(int pageId) {
		return map.get(pageId);
	}
	
	public int write(PagedBTreeNode node) {
		pageId++;
		map.put(pageId, node);
		return pageId;
	}

}
