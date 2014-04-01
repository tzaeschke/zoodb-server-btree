package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.MemoryBTreeNodeFactory;

/**
 * Abstracts the need to specify a BTreeNodeFactory.
 */
public class UniqueMemoryBTree extends UniqueBTree {

    public UniqueMemoryBTree(int order) {
        super(order, new MemoryBTreeNodeFactory());
    }

    @Override
    protected void markChanged(BTreeNode node) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
