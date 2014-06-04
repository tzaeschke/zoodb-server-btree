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

import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

public class PagedBTreeNodeFactory implements BTreeNodeFactory {

	private BTreeBufferManager bufferManager;

	public PagedBTreeNodeFactory(BTreeBufferManager bufferManager) {
		this.bufferManager = bufferManager;
	}

	@Override
	public PagedBTreeNode newUniqueNode(int pageSize, boolean isLeaf, boolean isRoot) {
		return new UniquePagedBTreeNode(bufferManager, pageSize, isLeaf, isRoot);
	}

    @Override
    public PagedBTreeNode newNonUniqueNode(int pageSize, boolean isLeaf, boolean isRoot) {
        return new NonUniquePagedBTreeNode(bufferManager, pageSize, isLeaf, isRoot);
    }

    @Override
    public PagedBTreeNode newNode(boolean isUnique, int pageSize, boolean isLeaf, boolean isRoot) {
        if (isUnique) {
            return newUniqueNode(pageSize, isLeaf, isRoot);
        } else {
            return newNonUniqueNode(pageSize, isLeaf, isRoot);
        }
    }

    public static PagedBTreeNode constructLeaf( BTreeBufferManager bufferManager,
                                                boolean isUnique,
                                                boolean isRoot,
                                                int pageSize,
                                                int pageId,
                                                int numKeys,
                                                long[] keys,
                                                long[] values) {
        boolean isLeaf = true;
        PagedBTreeNode node = createNode(bufferManager, isUnique, isRoot, isLeaf, pageSize, pageId);

		node.setNumKeys(numKeys);
		node.setKeys(keys);
		node.setValues(values);
		node.recomputeSize();
		return node;
	}
	
    public static PagedBTreeNode constructInnerNode( BTreeBufferManager bufferManager,
                                                     boolean isUnique,
                                                     boolean isRoot,
                                                     int pageSize,
                                                     int pageId,
                                                     int numKeys,
                                                     long[] keys,
                                                     long[] values,
                                                     int[] childrenPageIds) {
        boolean isLeaf = false;
		PagedBTreeNode node = createNode(bufferManager, isUnique, isRoot, isLeaf, pageSize, pageId);

		node.setNumKeys(numKeys);
		node.setKeys(keys);
        if (values != null) {
            node.setValues(values);
        }
		node.setChildrenPageIds(childrenPageIds);
		node.recomputeSize();
		return node;
	}

    private static PagedBTreeNode createNode(   BTreeBufferManager bufferManager,
                                                boolean isUnique,
                                                boolean isRoot,
                                                boolean isLeaf,
                                                int pageSize,
                                                int pageId) {
        PagedBTreeNode node;
        if (isUnique) {
            node = new UniquePagedBTreeNode(bufferManager, pageSize, isLeaf, isRoot, pageId);
        } else {
            node = new NonUniquePagedBTreeNode(bufferManager, pageSize, isLeaf, isRoot, pageId);
        }
        return node;
    }
}
