package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeHashBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TestNonUnique {

    private BTreeBufferManager bufferManager = new BTreeHashBufferManager();

    @Test
    public void testSameKey() {
        int order = 5;
        BTree tree = factory(order).getTree();
        tree.insert(1, 1);
        tree.insert(1, 2);
        assertEquals(2, tree.getRoot().getNumKeys());

    }

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
        NonUniquePagedBTree tree = (NonUniquePagedBTree) factory.getTree();
        tree.insert(1, 1);
        tree.insert(2, 2);
        tree.insert(2, 3);

        tree.delete(2, 3);
        factory.clear();
        factory.addLeafLayerDefault(
                Arrays.asList(
                        Arrays.asList(1L, 2L)
                )
        );
        BTree expected = factory.getTree();
        assertEquals(expected, tree);
    }

    @Test
    public void testInsertSplit() {
        int order = 5;
        BTreeFactory factory = factory(order);
        NonUniquePagedBTree tree = (NonUniquePagedBTree) factory.getTree();

        for (int i = 0; i < 10; i++) {
            tree.insert(1, i);
        }
        System.out.println(tree);
    }

    @Test
    public void testDeleteLeaf() {
        int order = 5;
        BTreeFactory factory = factory(order);
        NonUniquePagedBTree tree = (NonUniquePagedBTree) factory.getTree();

        tree.insert(1, 1);
        tree.insert(2, 2);
        tree.insert(2, 3);
        tree.insert(3, 3);

        tree.delete(2, 3);
        factory.clear();
        factory.addLeafLayerDefault(
                Arrays.asList(
                        Arrays.asList(
                                1L, 2L, 3L
                        )
                )
        );
        BTree expected = factory.getTree();
        assertEquals(expected, tree);
    }


    private BTreeFactory factory(int order) {
        boolean unique = false;
        return new BTreeFactory(order, bufferManager, unique);
    }
}
