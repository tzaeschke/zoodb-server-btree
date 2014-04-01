package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;

public class NonUniqueBTree extends BTree {

    public NonUniqueBTree(int order, BTreeNodeFactory nodeFactory) {
        super(order, nodeFactory);
    }

    @Override
    protected void markChanged(BTreeNode node) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void insert(long key, long value) {

    }

    public boolean contains(long key, long value) {
        return false;
    }

    public void delete(long key, long value) {

    }


}
