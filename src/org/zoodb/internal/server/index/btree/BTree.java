package org.zoodb.internal.server.index.btree;

/**
 * Shared behaviour of unique and non-unique B+ tree.
 */
public abstract class BTree<T extends BTreeNode> {

    protected int order;
    protected T root;
    protected BTreeNodeFactory nodeFactory;

    public abstract void insert(long key, long value);

    public BTree(int order, BTreeNodeFactory nodeFactory) {
        this.order = order;
        this.nodeFactory = nodeFactory;
    }

    public void setRoot(BTreeNode root) {
        this.root = (T) root;
    }

    public boolean isEmpty() {
        return root==null;
    }

    public int getOrder() {
        return this.order;
    }

    public T getRoot() {
        return this.root;
    }

    public String toString() {
        if(this.root != null) {
            return this.root.toString();
        } else {
            return "Empty tree";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BTree)) return false;

        BTree tree = (BTree) o;

        if (order != tree.getOrder()) return false;
        if (root != null ? !root.equals(tree.getRoot()) : tree.getRoot() != null) return false;

        return true;
    }

    public void swapRoot(T newRoot) {
        if (root != null) {
            root.setIsRoot(false);
        }
        root = newRoot;
        if (newRoot != null) {
            newRoot.setIsRoot(true);
        }
    }

    public BTreeNodeFactory getNodeFactory() {
        return nodeFactory;
    }



}
