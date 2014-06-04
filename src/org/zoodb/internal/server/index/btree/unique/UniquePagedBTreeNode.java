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
package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;

/**
 * Corresponds to Unique B+ trees.
 */
public class UniquePagedBTreeNode extends PagedBTreeNode {

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot) {
        super(bufferManager, pageSize, isLeaf, isRoot);
    }

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, pageSize, isLeaf, isRoot, pageId);
    }

    @Override
    protected void copyValues(PagedBTreeNode node) {
        //do nothing
    }

    @Override
    public void initializeEntries() {
        int size = computeMaxPossibleEntries();
        initKeys(size);
        if (!isLeaf()) {
            initChildren(size + 1);
        } else {
            initValues(size);
        }
    }
    
    public int computeMaxPossibleEntries() {
    	return PagedBTreeNode.computeMaxPossibleEntries(true, isLeaf(), pageSize, valueElementSize);
    }

    @Override
    public UniquePagedBTreeNode newNode(int pageSize, boolean isLeaf, boolean isRoot) {
        return new UniquePagedBTreeNode(bufferManager, pageSize, isLeaf, isRoot);
    }

    @Override
    public void migrateEntry(int destinationPos, BTreeNode source, int sourcePos) {
        long key = source.getKey(sourcePos);
        setKey(destinationPos, key);

        markDirty();
    }

    @Override
    public void setEntry(int pos, long key, long value) {
        setKey(pos, key);
    }

    @Override
    public void copyFromNodeToNode(int srcStartK, int srcStartC, BTreeNode destination, int destStartK, int destStartC, int keys, int children) {
        BTreeNode source = this;
        System.arraycopy(source.getKeys(), srcStartK, destination.getKeys(), destStartK, keys);
        if (destination.isLeaf()) {
            System.arraycopy(source.getValues(), srcStartK, destination.getValues(), destStartK, keys);
        } else {
            source.copyChildren(source, srcStartC, destination, destStartC, children);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecords(int startIndex, int endIndex, int amount) {
        shiftKeys(startIndex, endIndex, amount);
        if (isLeaf()) {
            shiftValues(startIndex, endIndex, amount);
        } else {
            shiftChildren(startIndex, endIndex, amount + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsRight(int amount) {
        shiftKeys(0, amount, getNumKeys());
        if (isLeaf()) {
            shiftValues(0, amount, getNumKeys());
        } else {
            shiftChildren(0, amount, getNumKeys() + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsLeftWithIndex(int startIndex, int amount) {
        int keysToMove = getNumKeys() - amount - startIndex;
        shiftKeys(startIndex + amount, startIndex, keysToMove);
        if (isLeaf()) {
            shiftValues(startIndex + amount, startIndex, keysToMove);
        } else {
            shiftChildren(startIndex + amount, startIndex, keysToMove + 1);
        }
        
        markDirty();
    }

    @Override
    protected boolean containsAtPosition(int position, long key, long value) {
        return this.getKey(position) == key;
    }

    @Override
    protected boolean smallerThanKeyValue(int position, long key, long value) {
        return (key < getKey(position));
    }
    
    @Override
    protected boolean allowNonUniqueKeys() {
    	return false;
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
        if (isLeaf()) {
            ret += ",   \tv:";
            ret += "[";
            for (int i = 0; i < this.getNumKeys(); i++) {
                ret += Long.toString(getValue(i));
                if (i != this.getNumKeys() - 1)
                    ret += " ";
            }
            ret += "]";
        } else {
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
    public long getNonKeyEntrySizeInBytes(int numKeys) {
        if (isLeaf()) {
            return numKeys * getValueElementSize();
        } else {
            return (numKeys + 1) << 2;
        }
    }
}
