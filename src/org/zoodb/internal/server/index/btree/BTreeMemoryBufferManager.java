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
package org.zoodb.internal.server.index.btree;

import java.util.HashMap;
import java.util.Map;

/**
 * BufferManager without using a proper storage for testing purposes.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
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
	public void remove(PagedBTreeNode node) {
		map.remove(node.getPageId());
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

	@Override
	public long getTxId() {
		//currently not supported
		return -1;
	}
}
