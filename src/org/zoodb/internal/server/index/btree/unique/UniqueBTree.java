package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.server.index.btree.BTreeUtils;
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
public class UniqueBTree extends BTree {

    public UniqueBTree(int order, BTreeNodeFactory nodeFactory) {
        super(order, nodeFactory);
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
     * @param key               The new key to be inserted
     * @param value         The new value to be inserted
     */
    public void insert(long key, long value) {
        if (root == null) {
            root = nodeFactory.newUniqueNode(order, true, true);
        }
        Pair<LinkedList<BTreeNode>, BTreeNode> result = searchNodeWithHistory(key);
        LinkedList<BTreeNode> ancestorStack = result.getA();
        BTreeNode leaf = result.getB();

        if (leaf.getNumKeys() < order - 1) {
            leaf.put(key, value);
            markChanged(leaf);
        } else {
            //split node
            BTreeNode rightNode = leaf.putAndSplit(key, value);
            markChanged(rightNode);
            insertInInnerNode(leaf, rightNode.getSmallestKey(), rightNode, ancestorStack);
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
    private void insertInInnerNode(BTreeNode left, long key, BTreeNode right, LinkedList<BTreeNode> ancestorStack) {
        if (left.isRoot()) {
            BTreeNode newRoot = nodeFactory.newUniqueNode(order, false, true);
            swapRoot(newRoot);
            root.put(key, left, right);
            markChanged(newRoot);
        } else {
            BTreeNode parent = ancestorStack.pop();
            markChanged(parent);
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
     *
     * Deletion steps are as a follows:
     *  - find the leaf node that contains the key that needs to be deleted.
     *  - delete the entry from the leaf
     *  - at this point, it is possible that the leaf is underfull.
     *    In this case, one of the following things are done:
     *    - if the left sibling has extra keys (more than the minimum number), borrow keys from the left node
     *    - if that is not possible, try to borrow extra keys from the right sibling
     *    - if that is not possible, either both the left and right nodes have precisely half the max number of keys.
     *      The current node has half the max number of keys - 1 so a merge can be done with either of them.
     *      The left node is check for merge first, then the right one.
     *
     * @param key               The key to be deleted.
     */
	public void delete(long key) {
		Pair<LinkedList<BTreeNode>,BTreeNode> pair = searchNodeWithHistory(key);
        BTreeNode leaf = pair.getB();
        LinkedList<BTreeNode> ancestorStack = pair.getA();
        deleteFromLeaf(leaf, key);
        markChanged(leaf);

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
                markChanged(leftSibling);
            } else if (rightSibling != null && rightSibling.hasExtraKeys()) {
                BTreeUtils.redistributeKeysFromRight(current, rightSibling, parent);
                markChanged(rightSibling);
            } else {
                //at this point, both left and right sibling have the minimum number of keys
                if (leftSibling!= null) {
                    //merge with left sibling
                    parent = BTreeUtils.mergeWithLeft(this, current, leftSibling, parent);
                    markChanged(leftSibling);
                } else {
                    //merge with right sibling
                    parent = BTreeUtils.mergeWithRight(this, current, rightSibling, parent);
                    markChanged(rightSibling);
                }
            }
            if (current.containsKey(key)) {
                current.replaceKey(key, replacementKey);
            }
            current = parent;
            parent = (ancestorStack.size() == 0 ) ? null : ancestorStack.pop();
        }
	}

    protected Pair<LinkedList<BTreeNode>, BTreeNode> searchNodeWithHistory(long key) {
        LinkedList<BTreeNode> stack = new LinkedList<>();
        BTreeNode current = root;
        while (!current.isLeaf()) {
            stack.push(current);
            current = current.findChild(key);
        }
        return new Pair<>(stack, current);
    }

    private void deleteFromLeaf(BTreeNode leaf, long key) {
        leaf.delete(key);
    }
}
