package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public abstract class BTreeLeafEntryIterator<T extends BTreeNode> implements
		LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> {
	protected BTree<T> tree;
	protected T curLeaf;
	protected int curPos;
	protected LinkedList<T> ancestors;

    protected long start = Long.MIN_VALUE;
    protected long end = Long.MAX_VALUE;

    abstract void updatePosition();
    abstract void setFirstLeaf();

	public BTreeLeafEntryIterator(BTree<T> tree) {
		this.tree = tree;
		this.curLeaf = null;
		this.curPos = -1;
		this.ancestors = new LinkedList<>();
        tree.registerIterator(this);
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
		return curLeaf != null && !tree.isEmpty() && tree.getRoot().getNumKeys() > 0;
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
