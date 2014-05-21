package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.DBLogger;

import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public abstract class BTreeLeafEntryIterator<T extends BTreeNode> implements
		LongLongIndex.LLEntryIterator {
	protected BTree<T> tree;
	protected T curLeaf;
	protected int curPos;
	protected LinkedList<T> ancestors;
    protected LinkedList<Integer> positions;

    protected long start = Long.MIN_VALUE;
    protected long end = Long.MAX_VALUE;
    
    // used to throw errors when modifying the
    // tree while using the iterator
    private final int modCount;
    private final Long txId; 

    abstract void updatePosition();
    abstract void setFirstLeaf();

	public BTreeLeafEntryIterator(BTree<T> tree) {
		this.tree = tree;
		this.modCount = tree.getModcount();
		this.txId = this.getTxId();
		this.curLeaf = null;
		this.curPos = -1;
		this.ancestors = new LinkedList<>();
        this.positions = new LinkedList<>();
		setFirstLeaf();
	}

    public BTreeLeafEntryIterator(BTree<T> tree, long start, long end) {
        this(tree);
        //ToDo get smallest key and value from tree
        this.start = start;
        this.end = end;
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

    protected T getLefmostLeaf(T node) {
        if(node.isLeaf()) {
            return node;
        }
        T current = node;
        while(!current.isLeaf()) {
            ancestors.push(current);
            positions.push(0);
            current = (T) current.getChild(0);
        }
        return current;
    }

    protected T getRightMostLeaf(T node) {
        if(node.isLeaf()) {
            return node;
        }
        T current = node;
        while(!current.isLeaf()) {
            ancestors.push(current);
            int numKeys = current.getNumKeys();
            positions.push(numKeys);
            current = (T) current.getChild(numKeys);
        }
        return current;
    }
    
	public void checkValidity() {
		if(this.txId != getTxId()) {
            throw DBLogger.newUser("This iterator has been invalidated by commit() or rollback().");
		}
		if (this.modCount != tree.getModcount()) {
			throw new ConcurrentModificationException();
		}
	}
	
	public Long getTxId() {
		// txId is only relevant when we are dealing with PagedBTrees on
		// StorageBufferManagers
		if (this.tree instanceof PagedBTree) {
			PagedBTree<?> tree = (PagedBTree<?>) this.tree;
            if(tree.getBufferManager() instanceof BTreeStorageBufferManager) {
                BTreeStorageBufferManager bm = (BTreeStorageBufferManager) tree.getBufferManager();
                return bm.getStorageFile().getTxId();
            }
		}
        return null;
	}
}
