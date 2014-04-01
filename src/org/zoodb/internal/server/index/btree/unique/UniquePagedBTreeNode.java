package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;

/**
 * Corresponds to Unique B+ tree indices.
 */
public class UniquePagedBTreeNode extends PagedBTreeNode {

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
        super(bufferManager, order, isLeaf, isRoot);
    }

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, order, isLeaf, isRoot, pageId);
    }

    @Override
    public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
        return new UniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
    }
}
