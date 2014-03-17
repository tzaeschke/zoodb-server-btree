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
            BTreeNode rightNode = leaf.putAndSplit(key, value);
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
                BTreeNode newNode = pair.getA();
                long keyToMoveUp = pair.getB();
                insertInInnerNode(parent, keyToMoveUp, newNode);
            }
        }
    }

    /**
     * Delete the value corresponding to the key from the tree.
     * @param key
     */
	public void delete(long key) {
		BTreeNode leaf = searchNode(key);
        deleteFromLeaf(leaf, key);
        if (leaf.isRoot()) {
            return;
        }
        BTreeNode current = leaf;
        while (current != null && current.isUnderfull()) {
            //check if can borrow 1 value from the left or right siblings
            if (current.leftSibling() != null && current.leftSibling().hasExtraKeys()) {
                redistributeKeysFromLeft(current, current.leftSibling());
            } else if (current.rightSibling() != null && current.rightSibling().hasExtraKeys()) {
                redistributeKeysFromRight(current, current.rightSibling());
            } else {
                //at this point, both left and right sibling have the minimum number of keys
                if (current.leftSibling() != null) {
                    mergeWithLeft(current, current.leftSibling());
                    //merge with left sibling
                } else {
                    //merge with right sibling
                    mergeWithRight(current, current.rightSibling());
                }
            }
            current = current.getParent();
        }
	}

    private void mergeWithRight(BTreeNode current, BTreeNode right) {
        BTreeNode parent = right.getParent();
        int keyIndex = parent.keyIndexOf(current, right);
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            right.shiftRecordsRight(parent.getNumKeys());
            copyFromNodeToNode(parent, 0, right, 0, parent.getNumKeys(), 0);
            right.increaseNumKeys(parent.getNumKeys());

            right.shiftRecordsRight(current.getNumKeys());
            copyFromNodeToNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys() + 1);
            right.increaseNumKeys(current.getNumKeys());
            setRoot(right);
            right.setParent(null);
        } else {
            parent.shiftRecordsLeftWithIndex(keyIndex, 1);
            parent.decreaseNumKeys(1);
            right.shiftRecordsRight(current.getNumKeys());
            copyFromNodeToNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys() + 1);
            right.increaseNumKeys(current.getNumKeys());
        }
    }

    private void mergeWithLeft(BTreeNode current, BTreeNode left) {
        BTreeNode parent = current.getParent();
        int keyIndex = parent.keyIndexOf(left, current);

        if (parent.isRoot() && parent.getNumKeys() == 1) {
            current.shiftRecordsRight(parent.getNumKeys());
            copyFromNodeToNode(parent, 0, current, 0, parent.getNumKeys(), 0);
            current.increaseNumKeys(parent.getNumKeys());

            current.shiftRecordsRight(left.getNumKeys());
            copyFromNodeToNode(left, 0, current, 0, left.getNumKeys(), left.getNumKeys() + 1);
            current.increaseNumKeys(left.getNumKeys());
            setRoot(current);
            current.setParent(null);
        } else {
            parent.shiftRecordsLeftWithIndex(keyIndex, 1);
            parent.decreaseNumKeys(1);

            current.shiftRecordsRight(left.getNumKeys());
            copyFromNodeToNode(left, 0, current, 0, left.getNumKeys(), left.getNumKeys() + 1);
            current.increaseNumKeys(left.getNumKeys());
        }
    }

    private void redistributeKeysFromRight(BTreeNode current, BTreeNode right) {
        int totalKeys = right.getNumKeys() + current.getNumKeys();
        int keysToMove = right.getNumKeys() - (totalKeys / 2);

        int startIndexRight = 0;
        int startIndexLeft = current.getNumKeys();
        //copy from left to current
        copyFromNodeToNode(right, startIndexRight, current, startIndexLeft, keysToMove, keysToMove + 1);

        //shift nodes in current node right
        right.shiftRecordsLeft(keysToMove);
        //fix number of keys
        right.decreaseNumKeys(keysToMove);
        current.increaseNumKeys(keysToMove);

        //move key from parent to current node
        BTreeNode parent = current.getParent();
        int parentKeyIndex = parent.keyIndexOf(current, right);
        if (current.isLeaf()) {
            parent.setKey(parentKeyIndex, right.getSmallestKey());
        } else {
            long aux = current.getLargestKey();
            current.setKey(keysToMove, parent.getKey(parentKeyIndex));
            parent.setKey(parentKeyIndex, aux);
        }
    }

    private void redistributeKeysFromLeft(BTreeNode current, BTreeNode left) {
        int totalKeys = left.getNumKeys() + current.getNumKeys();
        int keysToMove = left.getNumKeys() - (totalKeys / 2) - 1;
        //shift nodes in current node right
        current.shiftRecordsRight(keysToMove + 1);

        int startIndexLeft = totalKeys / 2 + 1;
        int startIndexRight = 0;
        //copy from left to current
        copyFromNodeToNode(left, startIndexLeft, current, startIndexRight, keysToMove, keysToMove + 1);

        //fix number of keys
        left.decreaseNumKeys(keysToMove + 1);
        current.increaseNumKeys(keysToMove + 1);

        //move key from parent to current node
        BTreeNode parent = current.getParent();
        int parentKeyIndex = parent.keyIndexOf(left, current);
        if (current.isLeaf()) {
            parent.setKey(parentKeyIndex, current.getSmallestKey());
        } else {
            long aux = current.getSmallestKey();
            current.setKey(keysToMove, parent.getKey(parentKeyIndex));
            parent.setKey(parentKeyIndex, aux);
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

    private void copyFromNodeToNode(BTreeNode source, int sourceStartIndex, BTreeNode destination, int destinationStartIndex, int keys, int children) {
        System.arraycopy(source.getKeys(), sourceStartIndex, destination.getKeys(), destinationStartIndex, keys);
        if (destination.isLeaf()) {
            System.arraycopy(source.getValues(), sourceStartIndex, destination.getValues(), destinationStartIndex, keys);
        } else {
            System.arraycopy(source.getChildren(), sourceStartIndex, destination.getChildren(), destinationStartIndex, children);
        }
    }

    private BTreeNode searchNode(long key) {
        return searchNode(root, key);
    }
    
    public boolean isEmpty() {
    	return root==null;
    }
    
    public int getOrder() {
    	return this.order;
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


}
