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

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.server.index.btree.*;

import java.util.List;


/**
 * Contains shared logic between {@link BTreeIndexUnique} and {@link BTreeIndexNonUnique}.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public abstract class BTreeIndex extends AbstractIndex {

    protected BTreeStorageBufferManager bufferManager;
    protected DATA_TYPE dataType;

	public BTreeIndex(DATA_TYPE dataType, StorageChannel file, boolean isNew, boolean isUnique) {
		super(file, isNew, isUnique);
        this.dataType = dataType;
        bufferManager = new BTreeStorageBufferManager(file, isUnique, dataType);
        this.dataType = dataType;
	}

	public void insertLong(long key, long value) {
		getTree().insert(key, value);
	}

    public boolean insertLongIfNotSet(long key, long value) {
        return getTree().insert(key, value, true);
    }

	public void print() {
        System.out.println(getTree());
	}

	public int statsGetLeavesN() {
		return getTree().statsGetLeavesN();
	}

	public int statsGetInnerN() {
		return getTree().statsGetInnerN();
	}

	public LLEntryIterator iterator() {
		return new AscendingBTreeLeafEntryIterator(getTree());
	}

	public LLEntryIterator iterator(long min, long max) {
        return new AscendingBTreeLeafEntryIterator(getTree(), min, max);
	}

	public LLEntryIterator descendingIterator() {
        return new DescendingBTreeLeafEntryIterator(getTree());
	}

	public LLEntryIterator descendingIterator(long max, long min) {
        return new DescendingBTreeLeafEntryIterator(getTree(), min, max);
	}

	public long getMinKey() {
		return getTree().getMinKey();
	}

	public long getMaxKey() {
		return getTree().getMaxKey();
	}

	public int write() {
		return bufferManager.write(getTree().getRoot());
	}

	public long size() {
		return getTree().size();
	}

	public DATA_TYPE getDataType() {
		return dataType;
	}

    public List<Integer> debugPageIds() {
		return bufferManager.debugPageIds(getTree());
	}

	public int statsGetWrittenPagesN() {
		return bufferManager.getStatNWrittenPages();
	}

    public abstract PagedBTree getTree();

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
	}

    public void setNodeValueSize(int sizeInByte) {
    	bufferManager.setNodeValueElementSize(sizeInByte);

    }
}
