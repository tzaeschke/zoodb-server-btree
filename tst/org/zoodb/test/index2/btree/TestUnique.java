package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeHashBufferManager;

public class TestUnique {

    private BTreeBufferManager bufferManager = new BTreeHashBufferManager();

    @Test(expected = IllegalStateException.class)
    public void testSameKeyPair() {
        int order = 5;
        BTree tree = factory(order).getTree();
        tree.insert(1, 1);
        tree.insert(1, 2);
    }

    private BTreeFactory factory(int order) {
        boolean unique = true;
        return new BTreeFactory(order, bufferManager, true);
    }

}
