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
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

import java.util.NoSuchElementException;

/**
 * Index backed by a B+ tree that does not allow duplicate keys.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public class BTreeIndexUnique extends BTreeIndex implements LongLongUIndex  {

    private UniquePagedBTree tree;
    
    public BTreeIndexUnique(DiskIO.PAGE_TYPE dataType, IOResourceProvider file) {
    	super(dataType, file, true, true);
    	initTree();
    }
    
    public BTreeIndexUnique(DiskIO.PAGE_TYPE dataType, int nodeValueSizeInByte, IOResourceProvider file) {
    	super(dataType, file, true, true);
    	bufferManager.setNodeValueElementSize(nodeValueSizeInByte);
    	initTree();
    }
    
	public BTreeIndexUnique(DiskIO.PAGE_TYPE dataType, IOResourceProvider file, int rootPageId) {
    	super(dataType, file, true, true);
    	loadTree(rootPageId);

    }
	
	public BTreeIndexUnique(DiskIO.PAGE_TYPE dataType, int nodeValueSizeInByte, IOResourceProvider file, int rootPageId) {
    	super(dataType, file, true, true);
    	bufferManager.setNodeValueElementSize(nodeValueSizeInByte);
    	loadTree(rootPageId);
    }
	
	public void initTree() {
		tree = new UniquePagedBTree(bufferManager.getPageSize(), bufferManager);
	}
	
	public void loadTree(int rootPageId) {
        UniquePagedBTreeNode root = (UniquePagedBTreeNode)bufferManager.read(rootPageId);
        root.setIsRoot(true);
		tree = new UniquePagedBTree(root, bufferManager.getPageSize(), bufferManager);
	}

    @Override
	public LLEntry findValue(long key) {
		Long value = tree.search(key);
		if (value != null) { 
            return new LLEntry(key, value);
		} else { 
			return null;
		}
	}

	@Override
	public long removeLong(long key) {
		return tree.delete(key);
	}
	
	@Override
    public long removeLong(long key, long value) {
		return getTree().delete(key);
	}

    @Override
	public boolean insertLongIfNotSet(long key, long value) {
        return tree.insert(key, value, true);
	}
    
    @Override
	public void clear() {
    	bufferManager.clear(tree.getRoot());
		tree = new UniquePagedBTree(tree.getPageSize(), bufferManager);
	}

	@Override
	public UniquePagedBTree getTree() {
		return tree;
	}

	@Override
	public long removeLongNoFail(long key, long failValue) {
		try {
			return removeLong(key);
		} catch (NoSuchElementException e) {
			return failValue;
		}
	}

	@Override
	public long deleteAndCheckRangeEmpty(long pos, long min, long max) {
		long ret = removeLong(pos);
		LongLongIterator<LLEntry> it = iterator(min, max);
		if(!it.hasNext()) {
			file.reportFreePage(BitTools.getPage(pos));
		}

		return ret;
	}

}
