package org.zoodb.internal.server.index.btree;

import java.util.Iterator;
import java.util.ListIterator;
import java.util.Stack;

/*
 * Iterates through all nodes (inner & leafs) of the tree in pre-order.
 */
public class BTreeIterator implements Iterator<BTreeNode> {
	private BTreeNode cur = null;
	private BTree tree;
	private Stack<BTreeNode> ancestors = new Stack<BTreeNode>();

	public BTreeIterator(BTree tree) {
		this.tree = tree;
	}

	/*
	 * Caution: unnecessarily SLOW!
	 */
	@Override
	public boolean hasNext() {
		if (tree.isEmpty()) {
			return false;
		} else if (cur == null) {
			return true;
		} else if (!cur.isLeaf()) {
			return cur.getNumKeys() > 0;
		} else /* if(cur.isLeaf()) */{
			BTreeNode tmp = cur;
			ListIterator<BTreeNode> ancestorIterator = ancestors
					.listIterator(ancestors.size());
			while (ancestorIterator.hasPrevious()) {
				BTreeNode ancestor = ancestorIterator.previous();
				if (tmp.rightSibling(ancestor) != null) {
					return true;
				}
				tmp = ancestor;
			}
			return false;
		}
	}

	@Override
	public BTreeNode next() {
		if (cur == null) {
			cur = tree.getRoot();
		} else {
			if (cur.isLeaf()) {
				BTreeNode rightSibling = null;
				BTreeNode ancestor = null;
				while (rightSibling == null && ancestors.size() > 0) {
					ancestor = ancestors.pop();
					rightSibling = cur.rightSibling(ancestor);
					cur = ancestor;
				}
				ancestors.push(ancestor);
				cur = rightSibling;
			} else {
				ancestors.push(cur);
				cur = cur.getChild(0);
			}
		}
		return cur;
	}

	@Override
	public void remove() {
		return;
	}

}
