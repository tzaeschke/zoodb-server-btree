package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

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
    private BTreeNodeFactory nodeFactory;

    public BTree(int order, BTreeNodeFactory nodeFactory) {
        this.order = order;
        this.nodeFactory = nodeFactory;
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
            root = nodeFactory.newNode(order, true, true);
        }
        Pair<LinkedList<BTreeNode>, BTreeNode> result = searchNodeWithHistory(key);
        LinkedList<BTreeNode> ancestorStack = result.getA();
        BTreeNode leaf = result.getB();

        if (leaf.getNumKeys() < order - 1) {
            leaf.put(key, value);
        } else {
            //split node
            BTreeNode rightNode = leaf.putAndSplit(key, value);
            insertInInnerNode(leaf, rightNode.getSmallestKey(), rightNode, ancestorStack);
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
    private void insertInInnerNode(BTreeNode left, long key, BTreeNode right, LinkedList<BTreeNode> ancestorStack) {
        if (left.isRoot()) {
            BTreeNode newRoot = nodeFactory.newNode(order, false, true);
            swapRoot(newRoot);
            root.put(key, left, right);
        } else {
            BTreeNode parent = ancestorStack.pop();
            //check if parent overflows
            if (parent.getNumKeys() < order - 1) {
                parent.put(key, right);
            } else {
                Pair<BTreeNode, Long > pair = parent.putAndSplit(key, right);
                BTreeNode newNode = pair.getA();
                long keyToMoveUp = pair.getB();
                insertInInnerNode(parent, keyToMoveUp, newNode, ancestorStack);
            }
        }
    }

    /**
     * Delete the value corresponding to the key from the tree.
     * @param key
     */
	public void delete(long key) {
		Pair<LinkedList<BTreeNode>,BTreeNode> pair = searchNodeWithHistory(key);
        BTreeNode leaf = pair.getB();
        LinkedList<BTreeNode> ancestorStack = pair.getA();
        deleteFromLeaf(leaf, key);

        if (leaf.isRoot()) {
            return;
        }
        long replacementKey = leaf.smallestKey();
        BTreeNode current = leaf;
        BTreeNode parent = (ancestorStack.size() == 0) ? null : ancestorStack.pop();
        while (current != null && current.isUnderfull()) {
            //check if can borrow 1 value from the left or right siblings
            BTreeNode rightSibling = current.rightSibling(parent);
            BTreeNode leftSibling = current.leftSibling(parent);
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
            if (current.containsKey(key)) {
                current.replaceKey(key, replacementKey);
            }
            current = parent;
            parent = (ancestorStack.size() == 0 ) ? null : ancestorStack.pop();
        }
	}

    private void deleteFromLeaf(BTreeNode leaf, long key) {
        leaf.delete(key);
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

    private Pair<LinkedList<BTreeNode>, BTreeNode> searchNodeWithHistory(long key) {
        LinkedList<BTreeNode> stack = new LinkedList<>();
        BTreeNode current = root;
        while (!current.isLeaf()) {
            stack.push(current);
            current = current.findChild(key);
        }
        return new Pair<>(stack, current);
    }
    
    public boolean isEmpty() {
    	return root==null;
    }
    
    public int getOrder() {
    	return this.order;
    }
    
    public BTreeNode getRoot() {
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

        BTree bTree = (BTree) o;

        if (order != bTree.order) return false;
        if (root != null ? !root.equals(bTree.root) : bTree.root != null) return false;

        return true;
    }

    void swapRoot(BTreeNode newRoot) {
        if (root != null) {
            root.setIsRoot(false);
        }
        root = newRoot;
        if (newRoot != null) {
            newRoot.setIsRoot(true);
        }
    }


}
