package org.zoodb.internal.server.index.btree;

import java.util.HashMap;
import java.util.Map;

public class BTreeHashBufferManager implements BTreeBufferManager {
	private Map<Integer, PagedBTreeNode> map;
	private int pageId;

	public BTreeHashBufferManager() {
		this.map = new HashMap<Integer, PagedBTreeNode>();
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

}
