package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

public class BTree {

    //order according to Knuth, max number of children
    private int order;
    private BTreeNode root;

    public BTree(int order) {
        this.order = order;
    }

    public void setRoot(BTreeNode root) {
        this.root = root;
    }

    public long search(long key) {
        BTreeNode current = root;
        while (!current.isLeaf()) {
            current = current.findChild(key);
        }
        return current.findValue(key);
    }

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
