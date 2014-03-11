package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

/**
 * B+ Tree data structure.
 *
 * The order parameter is considered according the Knuth, as the
 * maximum number of children in any node.
 *
 * Proper support for handling duplicate keys needs to be added.
 */
public class BTree {

    private int order;
    private BTreeNode root;

    public BTree(int order) {
        this.order = order;
    }

    public void setRoot(BTreeNode root) {
        this.root = root;
    }

    /**
     * Retrieve the value corresponding to the key from the B+ tree.
     *
     * @param key
     * @return
     */
    public long search(long key) {
        BTreeNode current = root;
        while (!current.isLeaf()) {
            current = current.findChild(key);
        }
        return current.findValue(key);
    }

    /**
     * Insert a new key value pair to the B+ tree.
     *
     * Algorithm performs as follows:
     *  - a reference to the leaf node on which the value for the key received as argument is first retrieved
     *  - if the leaf will not overflow after adding the new (key, value) pair, the pair is inserted in the leaf.
     *  - if the leaf will overflow after the addition of the new (key, value) pair, the leaf is split into 2 leaves.
     *    The keys/values in the original leaf are split as evenly as possible between the 2 leaves.
     *    The references to the parent node are then fixed.
     * @param key
     * @param value
     */
    public void insert(long key, long value) {
        if (root == null) {
            root = new BTreeNode(null, order, true);
        }
        BTreeNode leaf = searchNode(key);

        if (leaf.getNumKeys() < order - 1) {
            leaf.put(key, value);
        } else {
            //split node
            BTreeNode rightNode = leaf.split(key);
            if (key > leaf.largestKey()) {
                rightNode.put(key, value);
            } else {
                leaf.put(key, value);
            }
            insertInInnerNode(leaf, rightNode.getSmallestKey(), rightNode);
        }
    }

    /**
     * Inserts the nodes left and right into the parent node. The right node is a new node, created
     * as a result of a split.
     *
     * @param left
     * @param key
     * @param right
     */
    private void insertInInnerNode(BTreeNode left, long key, BTreeNode right) {
        if (left.isRoot()) {
            BTreeNode newRoot = new BTreeNode(null, order, false);
            root = newRoot;
            root.put(key, left, right);
        } else {
            BTreeNode parent = left.getParent();
            //check if parent overflows
            if (parent.getNumKeys() < order - 1) {
                parent.put(key, right);
            } else {
                Pair<BTreeNode, Long > pair = parent.putAndSplit(key, right);
                insertInInnerNode(parent, pair.getB(), pair.getA());
            }
        }
    }

    private BTreeNode searchNode(BTreeNode node, long key) {
        if (node.isLeaf())  {
            return node;
        }

        BTreeNode child = node.findChild(key);
        return searchNode(child, key);
    }

    private BTreeNode searchNode(long key) {
        return searchNode(root, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BTree)) return false;

        BTree bTree = (BTree) o;

        if (order != bTree.order) return false;
        if (root != null ? !root.equals(bTree.root) : bTree.root != null) return false;

        return true;
    }
}
