package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeHashBufferManager;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestNonUnique {

    private BTreeBufferManager bufferManager = new BTreeHashBufferManager();

    @Test(expected = IllegalStateException.class)
    public void testSameKeyValuePair() {
        int order = 5;
        BTree tree = factory(order).getTree();
        tree.insert(1, 1);
        tree.insert(1, 1);
    }

    @Test
    public void testSameKeyLeaf() {
        int order = 5;
        BTreeFactory factory = factory(order);
        BTree tree = factory.getTree();
        tree.insert(1, 1);
        tree.insert(2, 2);
        tree.insert(2, 3);

        factory.clear();
        factory.addInnerLayer(
                Arrays.asList(
                    Arrays.asList(1L, 2L, 2L)
                )
        );
        BTree expected = factory.getTree();
        assertEquals(expected, tree);
    }

    @Test
    public void testDeleteLeaf() {
        int order = 5;
        BTreeFactory factory = factory(order);
        BTree tree = factory.getTree();

        tree.insert(1, 1);
        tree.insert(2, 2);
        tree.insert(2, 3);
        tree.insert(3, 3);

        factory.clear();
        factory.addInnerLayer(
                Arrays.asList(
                        Arrays.asList(
                                1L, 2L, 3L
                        )
                )
        );
        BTree expected = factory.getTree();
        //ToDo add delete key, value

        assertEquals(expected, tree);
    }


    private BTreeFactory factory(int order) {
        return new BTreeFactory(order, bufferManager);
    }
}
