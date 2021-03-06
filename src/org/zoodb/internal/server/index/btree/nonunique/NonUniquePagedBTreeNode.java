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
package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;

/**
 * Node class for key-value unique trees.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public class NonUniquePagedBTreeNode extends PagedBTreeNode {

    public NonUniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
        super(bufferManager, order, isLeaf, isRoot);
    }

    public NonUniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, order, isLeaf, isRoot, pageId);
    }

    @Override
    public void initializeEntries() {
        int size = PagedBTreeNode.computeMaxPossibleEntries(false, isLeaf(), pageSize, valueElementSize);
        initKeys(size);
        initValues(size);
        if (!isLeaf()) {
            initChildren(size + 1);
        }
    }
    
    //TZ: Pure testing method
    public int computeMaxPossibleEntries() {
    	return PagedBTreeNode.computeMaxPossibleEntries(false, isLeaf(), pageSize, valueElementSize);
    }

    @Override
    public void migrateEntry(int destinationPos, BTreeNode source, int sourcePos) {
        long key = source.getKey(sourcePos);
        long value = source.getValue(sourcePos);
        setKey(destinationPos, key);
        setValue(destinationPos, value);

        markDirty();
    }

    @Override
    public void setEntry(int pos, long key, long value) {
        setKey(pos, key);
        setValue(pos, value);
    }

    @Override
    public void copyFromNodeToNode(int srcStartK, int srcStartC, BTreeNode destination, int destStartK, int destStartC, int keys, int children) {
        BTreeNode source = this;
        System.arraycopy(source.getKeys(), srcStartK, destination.getKeys(), destStartK, keys);
        System.arraycopy(source.getValues(), srcStartK, destination.getValues(), destStartK, keys);
        if (!destination.isLeaf()) {
            source.copyChildren(source, srcStartC, destination, destStartC, children);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecords(int startIndex, int endIndex, int amount) {
        shiftKeys(startIndex, endIndex, amount);
        shiftValues(startIndex, endIndex, amount);
        if (!isLeaf()) {
            shiftChildren(startIndex, endIndex, amount + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsRight(int amount) {
        shiftKeys(0, amount, getNumKeys());
        shiftValues(0, amount, getNumKeys());
        if (!isLeaf()) {
            shiftChildren(0, amount, getNumKeys() + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsLeftWithIndex(int startIndex, int amount) {
        int keysToMove = getNumKeys() - amount - startIndex;
        shiftKeys(startIndex + amount, startIndex, keysToMove);
        shiftValues(startIndex + amount, startIndex, keysToMove);
        if (!isLeaf()) {
            shiftChildren(startIndex + amount, startIndex, keysToMove + 1);
        }
        
        markDirty();
    }

    @Override
	public boolean smallerThanKeyValue(int position, long key, long value) {
        return (key < getKey(position) ||
                (key == getKey(position) && value < getValue(position)));
    }
    
    @Override
    protected boolean allowNonUniqueKeys() {
    	return true;
    }

    @Override
    public long getNonKeyEntrySizeInBytes(int numKeys) {
        if (isLeaf()) {
            return numKeys * getValueElementSize();
        } else {
            int numChildren = numKeys + 1;
            return numKeys * getValueElementSize() + (numChildren << 2);
        }
    }

    @Override
    public String toString() {
        String ret = (isLeaf() ? "leaf" : "inner") + "-node: k:";
        ret += "[";
        for (int i = 0; i < this.getNumKeys(); i++) {
            ret += Long.toString(getKey(i));
            if (i != this.getNumKeys() - 1)
                ret += " ";
        }
        ret += "]";
        ret += ",   \tv:";
        ret += "[";
        for (int i = 0; i < this.getNumKeys(); i++) {
            ret += Long.toString(getValue(i));
            if (i != this.getNumKeys() - 1)
                ret += " ";
        }
        ret += "]";

        if (!isLeaf()) {
            ret += "\n\tc:";
            if (this.getNumKeys() != 0) {
                for (int i = 0; i < this.getNumKeys() + 1; i++) {
                    String[] lines = this.getChild(i).toString()
                            .split("\r\n|\r|\n");
                    for (String l : lines) {
                        ret += "\n\t" + l;
                    }
                }
            }
        }
        return ret;
    }

	@Override
	public int binarySearch(long key, long value) {
		int low = 0;
		int high = this.getNumKeys() - 1;
		int mid = 0;
		while (low <= high) {
			mid = low + ((high - low) >> 1);
			if (getKey(mid) == key && getValue(mid) == value) {
				return mid;
			} else {
				if (key < getKey(mid) || (key == getKey(mid) && value < getValue(mid))) {
					high = mid - 1;
				} else {
					low = mid + 1;
				}
			}
		}
		return -mid - 1;
	}
}
