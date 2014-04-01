package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.btree.unique.UniqueBTree;

/**
 * Shared behaviour of unique and non-unique B+ tree.
 */
public abstract class BTree<T extends BTreeNode> {

    protected int order;
    protected T root;
    protected BTreeNodeFactory nodeFactory;

    public BTree(int order, BTreeNodeFactory nodeFactory) {
        this.order = order;
        this.nodeFactory = nodeFactory;
    }

    public void setRoot(T root) {
        this.root = root;
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
        if (!(o instanceof UniqueBTree)) return false;

        UniqueBTree uniqueBTree = (UniqueBTree) o;

        if (order != uniqueBTree.getOrder()) return false;
        if (root != null ? !root.equals(uniqueBTree.getRoot()) : uniqueBTree.getRoot() != null) return false;

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

    protected abstract void markChanged(BTreeNode node);

    public BTreeNodeFactory getNodeFactory() {
        return nodeFactory;
    }

}
