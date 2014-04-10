package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;

public class BTreeLeafIterator<T extends PagedBTreeNode> implements
		LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> {
	private BTree<T> tree;
	private T curLeaf;
	private int curPos;
	private LinkedList<T> ancestors;

    private int start = Integer.MIN_VALUE;
    private int end = Integer.MAX_VALUE;
    private HashMap<Integer, T> clones;

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
    }

	public void setFirstLeaf() {
		if (tree.isEmpty()) {
			return;
		}
		Pair<LinkedList<T>, T> p = tree
				.searchNodeWithHistory(Long.MIN_VALUE, 0);
		ancestors = p.getA();
		curLeaf = p.getB();
		curPos = 0;
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
			if (curPos < curLeaf.getNumKeys() - 1) {
				curPos++;
			} else {
				curPos = 0;
				T rightSibling = null;
				T ancestor = null;
				T ancestorsChild = curLeaf;
				while (rightSibling == null && ancestors.size() > 0) {
					ancestor = ancestors.pop();
					rightSibling = (T) ancestorsChild.rightSibling(ancestor);
					ancestorsChild = ancestor;
				}
				ancestors.push(ancestor);
				if (rightSibling == null) {
					curLeaf = null;
				} else {
					if (!rightSibling.isLeaf())
						ancestors.push(rightSibling);
					curLeaf = (T) rightSibling.leftMostLeafOf();
				}
			}
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

}
