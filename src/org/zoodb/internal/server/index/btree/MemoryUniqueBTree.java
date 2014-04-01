package org.zoodb.internal.server.index.btree;

/**
 * Abstracts the need to specify a BTreeNodeFactory.
 */
public class MemoryUniqueBTree extends UniqueBTree {

    public MemoryUniqueBTree(int order) {
        super(order, new MemoryBTreeNodeFactory());
    }
}
