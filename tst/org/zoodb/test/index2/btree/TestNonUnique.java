package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.nonunique.NonUniqueBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestNonUnique {

    private BTreeBufferManager bufferManager = new BTreeMemoryBufferManager();

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

        for (int i = 0; i < 100; i++) {
            tree.insert(1, i);
        }
        //System.out.println(tree);
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

    @Test
    public void testInsertAndDelete() {
        int order = 320;
        int numEntries = 100000;
        BTreeFactory factory = factory(order);
        NonUniqueBTree tree = (NonUniqueBTree) factory.getTree();
        List<LongLongIndex.LLEntry> entries = BTreeTestUtils.randomUniqueEntries(numEntries);

        for (LongLongIndex.LLEntry entry : entries) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        // check whether all entries are inserted
        for (LongLongIndex.LLEntry entry : entries) {
            assertTrue(tree.contains(entry.getKey(), entry.getValue()));
        }

        // delete every entry and check that there is indeed no entry anymore
        for (LongLongIndex.LLEntry entry : entries) {
            tree.delete(entry.getKey(), entry.getValue());
        }
        for (LongLongIndex.LLEntry entry : entries) {
            assertFalse(tree.contains(entry.getKey(), entry.getValue()));
        }

        // root is empty and has no children
        assertEquals(0, tree.getRoot().getNumKeys());
        BTreeNode[] emptyChildren = new BTreeNode[order];
        Arrays.fill(emptyChildren, null);
        assertArrayEquals(emptyChildren, tree.getRoot().getChildren());

        // add all entries, delete half of it, check that correct ones are
        // deleted and still present respectively
        int split = numEntries / 2;
        for (LongLongIndex.LLEntry entry : entries) {
            tree.insert(entry.getKey(), entry.getValue());
        }
        int i = 0;
        for (LongLongIndex.LLEntry entry : entries) {
            if (i < split) {
                tree.delete(entry.getKey(), entry.getValue());
            }
            i++;
        }
        i = 0;
        for (LongLongIndex.LLEntry entry : entries) {
            if (i < split) {
                assertFalse(tree.contains(entry.getKey(), entry.getValue()));
            } else {
                assertTrue(tree.contains(entry.getKey(), entry.getValue()));
            }
            i++;
        }
    }


    private BTreeFactory factory(int order) {
        boolean unique = false;
        return new BTreeFactory(order, bufferManager, unique);
    }
}
