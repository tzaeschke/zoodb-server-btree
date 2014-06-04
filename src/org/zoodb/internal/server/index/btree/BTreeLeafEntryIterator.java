package org.zoodb.internal.server.index.btree;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.FastStack;

public abstract class BTreeLeafEntryIterator implements
		LongLongIndex.LLEntryIterator {
	protected final BTree tree;
	protected BTreeNode curLeaf;
	protected int curPos;
	protected final FastStack<BTreeNode> ancestors;
    protected final FastStack<Integer> positions;

    protected final long min;
    protected final long max;
    
    // used to throw errors when modifying the
    // tree while using the iterator
    private final int modCount;
    private final long txId; 

    abstract void updatePosition();
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
        checkValidity();
		return  curLeaf != null && !tree.isEmpty() && tree.getRoot().getNumKeys() > 0;
	}

	@Override
	public LLEntry next() {
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
		return this.hasNext();
	}

	@Override
	public LongLongIndex.LLEntry nextULL() {
		return this.next();
	}

	@Override
	public long nextKey() {
		return this.next().getKey();
	}

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
		if (this.tree instanceof PagedBTree) {
			return ((PagedBTree)tree).getBufferManager().getTxId();
		}
		throw new UnsupportedOperationException(this.tree.getClass().getName());
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
