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
package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTree;

/**
 * key-value unique B+ Tree.
 *
 * It allows duplicate keys, but no duplicate key-value pairs.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public class NonUniquePagedBTree extends PagedBTree {

	public NonUniquePagedBTree(NonUniquePagedBTreeNode root,
			int pageSize, BTreeBufferManager bufferManager) {
		super(root, pageSize, bufferManager, false);
	}

    public NonUniquePagedBTree(int pageSize, BTreeBufferManager bufferManager) {
        super(pageSize, bufferManager, false);
    }

    public boolean contains(long key, long value) {
        BTreeNode current = root;
        while (!current.isLeaf()) {
            current = current.findChild(key, value);
        }
        return current.containsKeyValue(key, value);
    }

    /**
     * Delete the value corresponding to the key from the tree.
     *
     * Deletion steps are as a follows:
     *  - find the leaf node that contains the key that needs to be deleted.
     *  - delete the entry from the leaf
     *  - at this point, it is possible that the leaf is underfull.
     *    In this case, one of the following things are done:
     *    - if the left sibling has extra keys (more than the minimum number), borrow keys from the left node
     *    - if that is not possible, try to borrow extra keys from the right sibling
     *    - if that is not possible, either both the left and right nodes have precisely half the max number of keys.
     *      The current node has half the max number of keys - 1 so a merge can be done with either of them.
     *      The left node is check for merge first, then the right one.
     *
     * @param key               The key to be deleted.
     */
    public long delete(long key, long value) {
        return deleteEntry(key, value);
    }
}
