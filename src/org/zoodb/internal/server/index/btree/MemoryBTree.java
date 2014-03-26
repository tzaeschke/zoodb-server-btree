package org.zoodb.internal.server.index.btree;

/**
 * Abstracts the need to specify a BTreeNodeFactory.
 */
public class MemoryBTree extends BTree {

    public MemoryBTree(int order) {
        super(order, new MemoryBTreeNodeFactory());
    }
}
