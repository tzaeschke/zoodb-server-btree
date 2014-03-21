package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.MemoryBTreeNode;

//Todo move to package org.zoodb.internal.server.index.btree
public class TrestBTreeNode {

    @Test
    public void findKeyPos() {
        BTreeNode tree = new MemoryBTreeNode(null, 6, true);

        tree.put(3, 1);
        tree.put(2, 5);
        tree.put(0, 5);
        tree.put(4, 10);
        tree.put(1, -100);

        for (int i = 0; i < 5; i++) {
            assertEquals("Failed to return the index + 1", i+1, tree.findKeyPos(i));
        }

        tree = new MemoryBTreeNode(null, 6, true);
        tree.put(3, 1);
        tree.put(1, 5);
        tree.put(5, 5);

        assertEquals("Failed to return the index + 1", 0, tree.findKeyPos(0));
        assertEquals("Failed to return the index + 1", 1, tree.findKeyPos(1));
        assertEquals("Failed to return the index + 1", 1, tree.findKeyPos(2));
        assertEquals("Failed to return the index + 1", 2, tree.findKeyPos(3));
        assertEquals("Failed to return the index + 1", 2, tree.findKeyPos(4));
        assertEquals("Failed to return the index + 1", 3, tree.findKeyPos(5));
        assertEquals("Failed to return the index + 1", 3, tree.findKeyPos(6));
    }

}
