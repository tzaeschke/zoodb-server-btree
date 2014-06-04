package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TestNonUnique {

    private int pageSize;

    public TestNonUnique(int pageSize) {
        this.pageSize = pageSize;
    }

    @Parameterized.Parameters
    public static Collection<?> data() {
        Object[][] data = new Object[][] { {128}, {256}, {512}};
        return Arrays.asList(data);
    }
    @Test
    public void testSameKey() {
        BTree tree = factory().getTree();
        tree.insert(1, 1);
        tree.insert(1, 2);
        assertEquals(2, tree.getRoot().getNumKeys());
    }

    @Test
    public void testSameKeyReverse() {
        BTree tree = factory().getTree();
        tree.insert(1, 3);
        tree.insert(1, 2);
        assertEquals(2, tree.getRoot().getNumKeys());
    }

    @Test
    public void testSameKeyValuePair() {
        BTree tree = factory().getTree();
        tree.insert(1, 1);
        tree.insert(1, 1);
        assertEquals(1, tree.getRoot().getNumKeys());
    }

    @Test
    public void testInsertSplit() {
        BTreeFactory factory = factory();
        NonUniquePagedBTree tree = (NonUniquePagedBTree) factory.getTree();

        for (int i = 0; i < 100; i++) {
            tree.insert(1, i);
        }
        System.out.println(tree);
    }

    @Test
    public void testInsertAndDelete() {
    	for (int i = 0; i < 5; i++) {
    		try {
    			testInsertAndDelete(i);
    		} catch (Throwable t) {
    			throw new RuntimeException("seed=" + i, t);
    		}
    	}
    }

    private void testInsertAndDelete(int seed) {
        int numEntries = 1000;
        int numTimes = 200;
        BTreeFactory factory = factory();
        NonUniquePagedBTree tree = (NonUniquePagedBTree) factory.getTree();
        List<LongLongIndex.LLEntry> entries = 
        		BTreeTestUtils.nonUniqueEntries(numEntries, numTimes, seed);

        for (LongLongIndex.LLEntry entry : entries) {
            tree.insert(entry.getKey(), entry.getValue());
            assertTrue(tree.contains(entry.getKey(), entry.getValue()));
        }

        // check whether all entries are inserted
        for (LongLongIndex.LLEntry entry : entries) {
            //ToDo check why it does not work
            assertTrue(tree.contains(entry.getKey(), entry.getValue()));
        }

        // delete every entry and check that there is indeed no entry anymore
        for (LongLongIndex.LLEntry entry : entries) {
            tree.delete(entry.getKey(), entry.getValue());
            assertFalse(tree.contains(entry.getKey(), entry.getValue()));
        }
        for (LongLongIndex.LLEntry entry : entries) {
            assertFalse(tree.contains(entry.getKey(), entry.getValue()));
        }

        // root is empty and has no children
        assertEquals(0, tree.getRoot().getNumKeys());
        assertTrue(tree.getRoot().isLeaf());
        assertArrayEquals(null, tree.getRoot().getChildrenPageIds());

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


    private BTreeFactory factory() {
        boolean unique = false;

        BTreeBufferManager bufferManager =
                new BTreeStorageBufferManager(new StorageRootInMemory(pageSize), false);
        return new BTreeFactory(bufferManager, unique);
    }
}
