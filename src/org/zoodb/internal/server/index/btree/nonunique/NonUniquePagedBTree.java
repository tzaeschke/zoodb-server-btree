package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;

public class NonUniquePagedBTree extends NonUniqueBTree<PagedBTreeNode> {

    private BTreeBufferManager bufferManager;

    public NonUniquePagedBTree(int order, BTreeBufferManager bufferManager) {
        super(order, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }

}
