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
package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;

/**
 * Index backed by a B+ tree that allows duplicate keys.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public class BTreeIndexNonUnique extends BTreeIndex implements LongLongIndex {

    private NonUniquePagedBTree tree;

    public BTreeIndexNonUnique(DiskIO.PAGE_TYPE dataType, IOResourceProvider file) {
        super(dataType, file, true, false);

        tree = new NonUniquePagedBTree(bufferManager.getPageSize(), bufferManager);
    }
    
    public BTreeIndexNonUnique(DiskIO.PAGE_TYPE dataType, IOResourceProvider file, int rootPageId) {
        super(dataType, file, true, false);
        
        NonUniquePagedBTreeNode root = (NonUniquePagedBTreeNode)bufferManager.read(rootPageId);
        root.setIsRoot(true);
        
        tree = new NonUniquePagedBTree(root, bufferManager.getPageSize(), bufferManager);
    }
    
    @Override
	public long removeLong(long key, long value) {
		return tree.delete(key, value);
	}

    @Override
    public void clear() {
    	bufferManager.clear(tree.getRoot());
		tree = new NonUniquePagedBTree(tree.getPageSize(), bufferManager);
    }

	public NonUniquePagedBTree getTree() {
		return tree;
	}

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
    }

}
