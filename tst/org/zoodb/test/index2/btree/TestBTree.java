package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeNode;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestBTree {

    @Test
    public void searchSingleNode() {
        final int order = 10;
        BTree tree = new BTree(order);

        Map<Long, Long> keyValueMap = BTreeTestUtils.randomUniformKeyValues(order / 2);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, Long> entry: keyValueMap.entrySet()) {
            long expectedValue = entry.getValue();
            long value = tree.search(entry.getKey());
            assertEquals("Incorrect value retrieved.", expectedValue, value);
        }
    }

    @Test
    public void searchAfterSplit() {
        final int order = 10000;
        BTree tree = new BTree(order);

        Map<Long, Long> keyValueMap = BTreeTestUtils.randomUniformKeyValues(order);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, Long> entry: keyValueMap.entrySet()) {
            long expectedValue = entry.getValue();
            long value = tree.search(entry.getKey());
            assertEquals("Incorrect value retrieved.", expectedValue, value);
        }
    }

    @Test
    public void searchMissingSingleNode() {
        final int order = 10000;
        BTree tree = new BTree(order);

        Map<Long, Long> keyValueMap = BTreeTestUtils.randomUniformKeyValues(order / 2);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        int[] missingKeys = { -1, order + 1};
        for (int key : missingKeys) {
            assertEquals("Incorrect return value when searching for missing key.", -1, tree.search(key));
        }
    }

    @Test
    public void searchMissingAfterSplit() {
        final int order = 10000;
        BTree tree = new BTree(order);

        Map<Long, Long> keyValueMap = BTreeTestUtils.randomUniformKeyValues(order);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        int[] missingKeys = { -1, order + 1};
        for (int key : missingKeys) {
            assertEquals("Incorrect return value when searching for missing key.", -1, tree.search(key));
        }
    }

    @Test
    public void insertWithSimpleSplit() {
        int order = 5;
        BTree tree = new BTree(order);
        tree.insert(3, 1);
        tree.insert(2, 5);
        tree.insert(0, 5);
        tree.insert(4, 10);
        tree.insert(1, -100);

        //build expected tree
        BTree expected = new BTree(order);
        BTreeNode root = new BTreeNode(null, order, false);
        root.setNumKeys(1);
        root.setKeys( padLongArray( new long[] { 3 }, order) );

        BTreeNode[] children = new BTreeNode[] {
            new BTreeNode(root, order, true),
            new BTreeNode(root, order, true)
        };

        children[0].setKeys( padLongArray( new long[] { 0, 1, 2 }, order) );
        children[0].setNumKeys(3);
        children[1].setKeys( padLongArray( new long[] { 3, 4 }, order));
        children[1].setNumKeys(2);
        expected.setRoot(root);

        assertEquals("Tree does not have the proper structure after insertion ", expected, tree);
    }

    @Test
    public void inserTwoLevelWithSplit() {
        BTree tree = treeInitialTwoLevelWithSplit();
        tree.insert(5, 5);
        BTree expected = treeExpectedTwoLevelWithSplit();
        assertEquals("Tree did not split properly after first insert.", expected, tree);
        tree.insert(85, 85);

        expected = treeExpectedFinalTwoLevelWithSplit();
        assertEquals("Tree did not split properly after second insert.", expected, tree);
    }

    public BTree treeInitialTwoLevelWithSplit() {
        int order = 4;
        BTree tree = new BTree(order);
        BTreeNode root = new BTreeNode(null, order, false);
        BTreeNode[] levelOneChildren = new BTreeNode[] {
                new BTreeNode(root, order, false),
                new BTreeNode(root, order, false)
        };
        root.setNumKeys(1);
        root.setKeys( padLongArray( new long[] { 90 }, order));
        root.setChildren( padChildrenArray( levelOneChildren, order ));

        levelOneChildren[0].setNumKeys(2);
        levelOneChildren[0].setKeys( padLongArray( new long[] { 40, 60 }, order));

        levelOneChildren[1].setNumKeys(1);
        levelOneChildren[1].setKeys( padLongArray( new long[] { 110 }, order));

        BTreeNode[] leaves = new BTreeNode[] {
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[1], order, true),
                new BTreeNode(levelOneChildren[1], order, true)
        };

        levelOneChildren[0].setChildren( padChildrenArray( new BTreeNode[] { leaves[0], leaves[1], leaves[2]}, order));
        levelOneChildren[1].setChildren( padChildrenArray( new BTreeNode[] { leaves[3], leaves[4]}, order));

        leaves[0].setNumKeys(3);
        leaves[0].setKeys( padLongArray( new long[] { 10, 20, 30 }, order));

        leaves[1].setNumKeys(2);
        leaves[1].setKeys( padLongArray( new long[] { 40, 50 }, order));

        leaves[2].setNumKeys(3);
        leaves[2].setKeys( padLongArray( new long[] { 60, 70, 80 }, order));

        leaves[3].setNumKeys(2);
        leaves[3].setKeys( padLongArray( new long[] { 90, 100 }, order));

        leaves[4].setNumKeys(2);
        leaves[4].setKeys( padLongArray( new long[] { 110, 120 }, order));

        tree.setRoot(root);
        return tree;
    }

    public BTree treeExpectedTwoLevelWithSplit() {
        int order = 4;
        BTree tree = new BTree(order);
        BTreeNode root = new BTreeNode(null, order, false);
        BTreeNode[] levelOneChildren = new BTreeNode[] {
                new BTreeNode(root, order, false),
                new BTreeNode(root, order, false)
        };
        root.setNumKeys(1);
        root.setKeys( padLongArray( new long[] { 90 }, order));
        root.setChildren( padChildrenArray( levelOneChildren, order ));

        levelOneChildren[0].setNumKeys(3);
        levelOneChildren[0].setKeys( padLongArray( new long[] { 20, 40, 60 }, order));

        levelOneChildren[1].setNumKeys(1);
        levelOneChildren[1].setKeys( padLongArray( new long[] { 110 }, order));

        BTreeNode[] leaves = new BTreeNode[] {
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[1], order, true),
                new BTreeNode(levelOneChildren[1], order, true)
        };

        levelOneChildren[0].setChildren( padChildrenArray( new BTreeNode[] { leaves[0], leaves[1], leaves[2], leaves[3]}, order));
        levelOneChildren[1].setChildren( padChildrenArray( new BTreeNode[] { leaves[4], leaves[5]}, order));

        leaves[0].setNumKeys(2);
        leaves[0].setKeys( padLongArray( new long[] { 5, 10 }, order));
        leaves[0].setValues( padLongArray( new long[] {5, 0, 0}, order));

        leaves[1].setNumKeys(2);
        leaves[1].setKeys( padLongArray( new long[] { 20, 30 }, order));

        leaves[2].setNumKeys(2);
        leaves[2].setKeys( padLongArray( new long[] { 40, 50 }, order));

        leaves[3].setNumKeys(3);
        leaves[3].setKeys( padLongArray( new long[] { 60, 70, 80 }, order));

        leaves[4].setNumKeys(2);
        leaves[4].setKeys( padLongArray( new long[] { 90, 100 }, order));

        leaves[5].setNumKeys(2);
        leaves[5].setKeys( padLongArray( new long[] { 110, 120 }, order));

        tree.setRoot(root);
        return tree;
    }

    public BTree treeExpectedFinalTwoLevelWithSplit() {
        int order = 4;
        BTree tree = new BTree(order);
        BTreeNode root = new BTreeNode(null, order, false);
        BTreeNode[] levelOneChildren = new BTreeNode[] {
                new BTreeNode(root, order, false),
                new BTreeNode(root, order, false),
                new BTreeNode(root, order, false),
        };
        root.setNumKeys(2);
        root.setKeys( padLongArray( new long[] { 60, 90 }, order));
        root.setChildren( padChildrenArray( levelOneChildren, order ));

        levelOneChildren[0].setNumKeys(2);
        levelOneChildren[0].setKeys( padLongArray( new long[] { 20, 40 }, order));

        levelOneChildren[1].setNumKeys(1);
        levelOneChildren[1].setKeys( padLongArray( new long[] { 80 }, order));

        levelOneChildren[2].setNumKeys(1);
        levelOneChildren[2].setKeys( padLongArray( new long[] { 110 }, order));


        BTreeNode[] leaves = new BTreeNode[] {
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[0], order, true),
                new BTreeNode(levelOneChildren[1], order, true),
                new BTreeNode(levelOneChildren[1], order, true),
                new BTreeNode(levelOneChildren[2], order, true),
                new BTreeNode(levelOneChildren[2], order, true)
        };

        levelOneChildren[0].setChildren( padChildrenArray( new BTreeNode[] { leaves[0], leaves[1], leaves[2]}, order));
        levelOneChildren[1].setChildren( padChildrenArray( new BTreeNode[] { leaves[3], leaves[4]}, order));
        levelOneChildren[2].setChildren( padChildrenArray( new BTreeNode[] { leaves[5], leaves[6]}, order));

        leaves[0].setNumKeys(2);
        leaves[0].setKeys( padLongArray( new long[] { 5, 10 }, order));
        leaves[0].setValues( padLongArray( new long[] {5, 0, 0}, order));

        leaves[1].setNumKeys(2);
        leaves[1].setKeys( padLongArray( new long[] { 20, 30 }, order));

        leaves[2].setNumKeys(2);
        leaves[2].setKeys( padLongArray( new long[] { 40, 50 }, order));

        leaves[3].setNumKeys(2);
        leaves[3].setKeys( padLongArray( new long[] { 60, 70}, order));

        leaves[4].setNumKeys(2);
        leaves[4].setKeys( padLongArray( new long[] { 80, 85 }, order));
        leaves[4].setValues( padLongArray( new long[] {0, 85}, order));

        leaves[5].setNumKeys(2);
        leaves[5].setKeys( padLongArray( new long[] { 90, 100 }, order));

        leaves[6].setNumKeys(2);
        leaves[6].setKeys( padLongArray( new long[] { 110, 120 }, order));

        tree.setRoot(root);
        return tree;
    }
    private long[] padLongArray(long[] keys, int order) {
        long[] paddedKeyArray = new long[order - 1];
        for (int i = 0; i < keys.length; i++) {
            paddedKeyArray[i] = keys[i];
        }
        for (int i = keys.length; i < order - 1; i++) {
            paddedKeyArray[i] = 0;
        }
        return paddedKeyArray;
    }

    private BTreeNode[] padChildrenArray(BTreeNode[] children, int order) {
        BTreeNode[] paddedNodeArray = new BTreeNode[order];
        for (int i = 0; i < children.length; i++) {
            paddedNodeArray[i] = children[i];
        }
        for (int i = children.length; i < order; i++) {
            paddedNodeArray[i] = null;
        }
        return paddedNodeArray;
    }


}
