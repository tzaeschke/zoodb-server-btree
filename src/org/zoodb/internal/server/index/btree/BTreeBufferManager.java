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

import org.zoodb.internal.server.StorageChannelOutput;

/**
 * 
 * @author bvancea, jonasnick
 */
public interface BTreeBufferManager {

	/**
	 * returns null if pageId can not be found
	 */
	public PagedBTreeNode read(int pageId);

	/**
	 * saves the node in the buffer manager
	 */
	public int save(PagedBTreeNode node);
	
    /**
	 * deletes a node from the buffer manager
	 */
	public void remove(PagedBTreeNode node);
	
    /**
	 * writes the node to the storage channel
	 */
	public int write(PagedBTreeNode node, StorageChannelOutput out);
	
	/**
	 * Update clean/dirty status of a node in the buffer manager. 
	 */
	public void updatePageStatus(PagedBTreeNode node);
	
    /**
	 * returns page size
	 */
	public int getPageSize();
	
	/**
	 * returns the size of the node in storage (including all metadata)
	 */
	public int getNodeSizeInStorage(PagedBTreeNode node);

    /**
     * return the size of the storage metadata for a node
     */
    public int getNodeHeaderSizeInStorage(PagedBTreeNode node);
    
    /**
     * returns the size in bytes of a nodes value element
     */
	public int getNodeValueElementSize();

    /**
	 * writes the node to the storage channel
	 */
	public void clear(PagedBTreeNode node);

    /**
	 * Get current transaction id.
	 */
	public long getTxId();

}
