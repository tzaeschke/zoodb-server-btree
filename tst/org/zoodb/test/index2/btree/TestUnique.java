package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;

public class TestUnique {

    private BTreeBufferManager bufferManager = new BTreeMemoryBufferManager();

    @Test
    public void testSameKeyPair() {
        int order = 5;
        UniquePagedBTree tree = (UniquePagedBTree) factory(order).getTree();
        tree.insert(1, 1);
        tree.insert(1, 2);
        System.out.println(tree);
        tree.insert(1, 3);
        System.out.println(tree);
        assertEquals(new Long(3), tree.search(1));
    }

    private BTreeFactory factory(int order) {
        boolean unique = true;
        return new BTreeFactory(order, bufferManager, true);
    }

}
