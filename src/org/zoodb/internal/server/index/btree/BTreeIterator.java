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
