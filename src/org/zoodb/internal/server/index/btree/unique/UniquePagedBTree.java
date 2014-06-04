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
package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTree;

/**
 * Abstracts the need to specify a BTreeNodeFactory, which is specific to this
 * type of tree.
 * 
 * Also, adds the buffer manager that will be used by this type of node as an
 * argument.
 */
public class UniquePagedBTree extends PagedBTree {
	
	private static final int NO_VALUE = 0;

    public UniquePagedBTree(UniquePagedBTreeNode root, int pageSize, BTreeBufferManager bufferManager) {
		super(root, pageSize, bufferManager, true);
	}
    public UniquePagedBTree(int pageSize, BTreeBufferManager bufferManager) {
        super(pageSize, bufferManager, true);
    }

	/**
	 * Retrieve the value corresponding to the key from the B+ tree.
	 * 
	 * @param key
	 * @return corresponding value or null if key not found
	 */
	public Long search(long key) {
		if (isEmpty()) {
			return null;
		}
		BTreeNode current = root;

		while (!current.isLeaf()) {
			current = current.findChild(key, NO_VALUE);
		}

        return findValue(current, key);
	}

	/**
	 * Delete the value corresponding to the key from the tree.
	 * 
	 * Deletion steps are as a follows: - find the leaf node that contains the
	 * key that needs to be deleted. - delete the entry from the leaf - at this
	 * point, it is possible that the leaf is underfull. In this case, one of
	 * the following things are done: - if the left sibling has extra keys (more
	 * than the minimum number), borrow keys from the left node - if that is not
	 * possible, try to borrow extra keys from the right sibling - if that is
	 * not possible, either both the left and right nodes have precisely half
	 * the max number of keys. The current node has half the max number of keys
	 * - 1 so a merge can be done with either of them. The left node is check
	 * for merge first, then the right one.
	 * 
	 * @param key
	 *            The key to be deleted.
	 */
	public long delete(long key) {
		return deleteEntry(key, NO_VALUE);
	}

	private Long findValue(BTreeNode node, long key) {
		if (!node.isLeaf()) {
			throw new IllegalStateException(
					"Should only be called on leaf nodes.");
		}
		if (node.getNumKeys() > 0) {
			int position = node.binarySearch(key, NO_VALUE);
			if (position >= 0) {
				return node.getValue(position);
			}
		}

        return null;
	}
}
