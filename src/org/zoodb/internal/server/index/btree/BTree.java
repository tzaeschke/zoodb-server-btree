package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

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

    public abstract boolean isUnique();

    /**
     * Insert a new key value pair to the B+ tree.
     *
     * Algorithm performs as follows:
     *  - a reference to the leaf node on which the value for the key received as argument is first retrieved
     *  - if the leaf will not overflow after adding the new (key, value) pair, the pair is inserted in the leaf.
     *  - if the leaf will overflow after the addition of the new (key, value) pair, the leaf is split into 2 leaves.
     *    The keys/values in the original leaf are split as evenly as possible between the 2 leaves.
     *    The references to the parent node are then fixed.
     * @param key               The new key to be inserted
     * @param value         The new value to be inserted
     */
    public void insert(long key, long value) {
        if (root == null) {
            root = (T) nodeFactory.newNode(isUnique(), order, true, true);
        }
        Pair<LinkedList<T>, T> result = searchNodeWithHistory(key, value);
        LinkedList<T> ancestorStack = result.getA();
        T leaf = result.getB();

        if (leaf.getNumKeys() < order - 1) {
            leaf.put(key, value);
        } else {
            //split node
            T rightNode = BTreeUtils.putAndSplit(leaf, key, value);
            insertInInnerNode(leaf, rightNode.getSmallestKey(), rightNode.getSmallestValue(),  rightNode, ancestorStack);
        }
    }

    protected void deleteEntry(long key, long value) {
        Pair<LinkedList<T>,T> pair = searchNodeWithHistory(key, value);
        T leaf = pair.getB();
        LinkedList<T> ancestorStack = pair.getA();
        deleteFromLeaf(leaf, key, value);

        if (leaf.isRoot()) {
            return;
        }
        long replacementKey = leaf.getSmallestKey();
        long replacementValue = leaf.getSmallestValue();
        T current = leaf;
        T parent = (ancestorStack.size() == 0) ? null : ancestorStack.pop();
        while (current != null && current.isUnderfull()) {
            //check if can borrow 1 value from the left or right siblings
            T rightSibling = current.rightSibling(parent);
            T leftSibling = current.leftSibling(parent);
            if (leftSibling != null && leftSibling.hasExtraKeys()) {
                BTreeUtils.redistributeKeysFromLeft(current, leftSibling, parent);
            } else if (rightSibling != null && rightSibling.hasExtraKeys()) {
                BTreeUtils.redistributeKeysFromRight(current, rightSibling, parent);
            } else {
                //at this point, both left and right sibling have the minimum number of keys
                if (leftSibling!= null) {
                    //merge with left sibling
                    parent = BTreeUtils.mergeWithLeft(this, current, leftSibling, parent);
                } else {
                    //merge with right sibling
                    parent = BTreeUtils.mergeWithRight(this, current, rightSibling, parent);
                }
            }
            if (BTreeUtils.containsKeyValue(current, key, value)) {
                BTreeUtils.replaceEntry(current, key, value, replacementKey, replacementValue);
            }
            current = parent;
            parent = (ancestorStack.size() == 0 ) ? null : ancestorStack.pop();
        }
    }

    /**
     * Inserts the nodes left and right into the parent node. The right node is a new node, created
     * as a result of a split.
     *
     * @param left              The left node.
     * @param key               The key that should separate them in the parent.
     * @param right             The right node.
     * @param ancestorStack     The ancestor stack that should be traversed.
     */
    private void insertInInnerNode(T left, long key, long value, T right, LinkedList<T> ancestorStack) {
        if (left.isRoot()) {
            T newRoot = (T) nodeFactory.newNode(isUnique(), order, false, true);
            swapRoot(newRoot);
            BTreeUtils.put(root, key, value, left, right);
        } else {
            T parent = ancestorStack.pop();
            //check if parent overflows
            if (parent.getNumKeys() < order - 1) {
                BTreeUtils.put(parent, key, value, right);
            } else {
                Pair<T, Pair<Long, Long> > pair = BTreeUtils.putAndSplit(parent, key, value, right);
                T newNode = pair.getA();
                Pair<Long, Long> keyValuePair = pair.getB();
                long keyToMoveUp = keyValuePair.getA();
                long valueToMoveUp = keyValuePair.getB();
                insertInInnerNode(parent, keyToMoveUp, valueToMoveUp, newNode, ancestorStack);
            }
        }
    }

    protected Pair<LinkedList<T>, T> searchNodeWithHistory(long key, long value) {
        LinkedList<T> stack = new LinkedList<>();
        T current = root;
        while (!current.isLeaf()) {
            stack.push(current);
            current = BTreeUtils.findChild(current, key, value);
        }
        return new Pair<>(stack, current);
    }

    protected void deleteFromLeaf(T leaf, long key, long value) {
        BTreeUtils.delete(leaf, key, value);
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
    
    /*
     * Counts number of nodes in the tree.
     * WARNING: SLOW! has to iterate over whole tree
     */
    public int size() {
    	BTreeIterator it = new BTreeIterator(this);
    	int counter = 0;
    	while(it.hasNext()) {
    		it.next();
    		counter++;
    	}

    	return counter;
    }
}
