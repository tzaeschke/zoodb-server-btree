package org.zoodb.internal.server.index.btree;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.Pair;

public class BTreeLeafIterator<T extends BTreeNode> implements
		LongLongIndex.LongLongIterator<LongLongIndex.LLEntry> {
	private BTree<T> tree;
	private T curLeaf;
	private int curPos;
	private LinkedList<T> ancestors;

	public BTreeLeafIterator(BTree<T> tree) {
		this.tree = tree;
		this.curLeaf = null;
		this.curPos = -1;
		this.ancestors = new LinkedList<T>();
		setFirstLeaf();
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
	}

	@Override
	public void refresh() {
		// TODO clear COW clones
		reset();
	}

	public void reset() {
		setFirstLeaf();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
