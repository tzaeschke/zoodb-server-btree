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

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.FastStack;

/**
 * An abstract iterator for iterating through entries of the leaf-nodes of a B+ tree.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public abstract class BTreeLeafEntryIterator implements
		LongLongIndex.LLEntryIterator {

	/**
	 * The B+ tree used for iteration.
	 */
	protected final BTree tree;

	/**
	 * The leaf node that contains the current iterator position.
	 */
	protected BTreeNode curLeaf;

	/**
	 * The index of the current entry in curLeaf.
	 */
	protected int curPos;
	
	/**
	 * The stack of ancestor nodes of the current leaf. Maintained because
	 * of the lack of a parent pointer
	 */
	protected final FastStack<BTreeNode> ancestors;

	/**
	 * The positions in the ancestor nodes.
	 */
    protected final FastStack<Integer> positions;

	/**
	 * The start of the key range used by the iterator.
	 */
	protected final long min;

	/**
	 * The end of the key range used by the iterator.
	 */
	protected final long max;
    
    // used to throw errors when modifying the
    // tree while using the iterator
    private final int modCount;
    private final long txId;

    /**
     * Update the position of the iterator.
     *
     * This method is implemented
     * by the AscendingBTreeLeafEntryIterator and DescendingBTreeLeafEntryIterator classes.
     */
    abstract void updatePosition();

    /**
     * Set the first leaf at the start of the iteration.
     *
     * This method is implemented
     * by the AscendingBTreeLeafEntryIterator and DescendingBTreeLeafEntryIterator classes.
     */
    abstract void setFirstLeaf();

	public BTreeLeafEntryIterator(BTree tree) {
		this(tree, Long.MIN_VALUE, Long.MAX_VALUE);
	}

    public BTreeLeafEntryIterator(BTree tree, long min, long max) {
		this.tree = tree;
		this.curLeaf = null;
		this.curPos = -1;
		this.ancestors = new FastStack<>();
		this.positions = new FastStack<>();
        this.modCount = tree.getModcount();
        this.txId = this.getTxId();
        //ToDo get smallest key and value from tree
        this.min = min;
        this.max = max;
        setFirstLeaf();
    }

	@Override
	public boolean hasNext() {
		return this.hasNextULL();
	}

	@Override
	public LLEntry next() {
		return this.nextULL();
	}

	@Override
	public void close() {
		// TODO release clones if there are any
		curLeaf = null;
	}

	public void reset() {
		setFirstLeaf();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean hasNextULL() {
        checkValidity();
		return curLeaf != null;
	}

	@Override
	public LongLongIndex.LLEntry nextULL() {
        checkValidity();
		if (curLeaf == null) {
			throw new NoSuchElementException();
		} else {
			LLEntry retEntry = new LLEntry(curLeaf.getKey(curPos),
					curLeaf.getValue(curPos));
            updatePosition();
			return retEntry;
		}
	}

	@Override
	public long nextKey() {
		return this.next().getKey();
	}

    /**
     *
     * @param node      An arbitrary node from the tree.
     * @return          The left-most leaf of the sub-tree rooted in node.
     */
    protected BTreeNode getLefmostLeaf(BTreeNode node) {
        if (node.isLeaf()) {
            return node;
        }
        BTreeNode current = node;
        while (!current.isLeaf()) {
            ancestors.push(current);
            positions.push(0);
            current = current.getChild(0);
        }
        return current;
    }

    /**
     *
     * @param node      An arbitrary node from the tree.
     * @return          The right-most leaf of the sub-tree rooted in node.
     */
    protected BTreeNode getRightMostLeaf(BTreeNode node) {
        if(node.isLeaf()) {
            return node;
        }
        BTreeNode current = node;
        while (!current.isLeaf()) {
            ancestors.push(current);
            int numKeys = current.getNumKeys();
            positions.push(numKeys);
            current = current.getChild(numKeys);
        }
        return current;
    }

    /**
     * Check if the current iterator is still valid.
     *
     * First checks if the transaction in which the iterator was created was commited or rolledback.
     *
     * The check if the tree was modified by comparing the modification counts.
     */
	public void checkValidity() {
		long storageTxId = getTxId();
		if (this.txId != storageTxId) {
            throw DBLogger.newUser("This iterator has been invalidated by commit() or rollback().");
		}
		if (this.modCount != tree.getModcount()) {
			throw new ConcurrentModificationException();
		}
	}
	
	public long getTxId() {
		// txId is only relevant when we are dealing with PagedBTrees on
		// StorageBufferManagers
		return ((PagedBTree)tree).getBufferManager().getTxId();
	}

	protected void populateAncestorStack(long key, long value) {
        BTreeNode current = tree.getRoot();
        int position;
        while (!current.isLeaf()) {
            position = current.findKeyValuePos(key, value);
        	
            //position = position > 0 ? position - 1 : 0;
            positions.push(position);
            ancestors.push(current);
            current = current.getChild(position);
        }
        curLeaf = current;
        curPos = curLeaf.findKeyValuePos(key, value);
    	// findKeyValuePos looks for a position to insert an entry
        // and thus it is one off
        curPos = curPos > 0 ? curPos-1 : 0; 
    }


}
