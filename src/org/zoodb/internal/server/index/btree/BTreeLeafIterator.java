package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.util.Pair;

import java.util.Arrays;
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
		curLeaf = accessNode(p.getB());
		curPos = 0;
	}

	@Override
	public boolean hasNext() {
		return curLeaf != null;
	}

	@Override
	public LLEntry next() {
        curLeaf = accessNode(curLeaf);
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
                    ancestor = accessNode(ancestor);
					rightSibling = (T) ancestorsChild.rightSibling(ancestor);
                    rightSibling = accessNode(rightSibling);
					ancestorsChild = ancestor;
				}
				ancestors.push(ancestor);
				if (rightSibling == null) {
					curLeaf = null;
				} else {
					if (!rightSibling.isLeaf())
						ancestors.push(rightSibling);
					curLeaf = accessNode((T) rightSibling.leftMostLeafOf());
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

    public void handleNodeChange(BTreeNode changedNode) {
        if (changedNode == null) {
            return;
        }
        T pagedNode = null;
        if (changedNode instanceof PagedBTreeNode) {
            pagedNode = (T) changedNode;
        } else {
            //ToDo should we throw an exception here?
            return;
        }
        if (!clones.containsKey(pagedNode.getPageId()) && isInterestedIn(pagedNode)) {
            clones.put(pagedNode.getPageId(), clone(pagedNode));
        }
    }

    private T clone(T original) {
        boolean isLeaf = original.isLeaf();
        T clone = null;

        long[] keys = Arrays.copyOf(original.getKeys(), original.getKeys().length);
        long[] values = (original.getValues() == null) ? null : Arrays.copyOf(original.getValues(), original.getValues().length);
        int[] childrenIds = (original.getChildrenPageIds() == null) ? null
                : Arrays.copyOf(original.getChildrenPageIds(), original.getChildrenPageIds().length);
        BTreeBufferManager bufferManager = original.getBufferManager();
        if (isLeaf) {
            clone = (T) PagedBTreeNodeFactory.constructLeaf(bufferManager, tree.isUnique(), original.isRoot(),
                    original.getOrder(), original.getPageId(), original.getNumKeys(), keys, values);
        } else {
            clone = (T) PagedBTreeNodeFactory.constructInnerNode(bufferManager, tree.isUnique(), original.isRoot(),
                    original.getOrder(), original.getPageId(), original.getNumKeys(), keys, values, childrenIds);
        }
        return clone;
    }

    private boolean isInterestedIn(T node) {
        long smallestKey = node.getSmallestKey();
        long largestKey = node.getLargestKey();
        return  (insideInterval(smallestKey, start, end) || insideInterval(largestKey, start, end));
    }

    private boolean insideInterval(long value, int start, int end) {
        return (value >= start || value <= end);
    }

    private T accessNode(T node) {
        if (node == null) {
            return null;
        }
        int pageId = node.getPageId();
        T clone = clones.get(pageId);
        return (clone == null) ? node : clone;
    }
}
