package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public abstract class BTreeLeafIterator<T extends PagedBTreeNode> implements
		LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> {
	protected BTree<T> tree;
	protected T curLeaf;
	protected int curPos;
	protected LinkedList<T> ancestors;

    protected int start = Integer.MIN_VALUE;
    protected int end = Integer.MAX_VALUE;
    protected HashMap<Integer, T> clones;

    abstract void updatePosition();
    abstract void setFirstLeaf();
    abstract void initializePosition();

	public BTreeLeafIterator(BTree<T> tree) {
		this.tree = tree;
		this.curLeaf = null;
		this.curPos = -1;
		this.ancestors = new LinkedList<>();
        this.clones = new HashMap<>();
        tree.registerIterator(this);
		setFirstLeaf();
	}

    public BTreeLeafIterator(BTree<T> tree, int start, int end) {
        this(tree);
        //ToDo get smallest key and value from tree
        this.start = start;
        this.end = end;
        initializePosition();
    }

	@Override
	public boolean hasNext() {
		return curLeaf != null;
	}

	@Override
	public LLEntry next() {
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
        tree.deregisterIterator(this);
	}

	@Override
	public void refresh() {
		clones.clear();
		reset();
	}

	public void reset() {
		setFirstLeaf();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

    public void handleNodeChange() {
        //TODO should we handle tree modifications differently?
        close();
    }

    protected T getLefmostLeaf(T node) {
        if(node.isLeaf()) {
            return node;
        }
        T current = node;
        while(!current.isLeaf()) {
            ancestors.push(current);
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
            current = (T) current.getChild(numKeys);
        }
        return current;
    }


}
