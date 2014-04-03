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
public abstract class UniqueBTree<T extends BTreeNode> extends BTree<T> {

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
        T current = root;
        while (!current.isLeaf()) {
            current = UniqueBTreeUtils.findChild(current, key);
        }
        return UniqueBTreeUtils.findValue(current, key);
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
        Pair<LinkedList<T>, T> result = searchNodeWithHistory(key);
        LinkedList<T> ancestorStack = result.getA();
        T leaf = result.getB();

        if (leaf.getNumKeys() < order - 1) {
            UniqueBTreeUtils.put(leaf, key, value);
            leaf.markChanged();
        } else {
            //split node
            T rightNode = UniqueBTreeUtils.putAndSplit(leaf, key, value);
            rightNode.markChanged();
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
    private void insertInInnerNode(T left, long key, T right, LinkedList<T> ancestorStack) {
        if (left.isRoot()) {
            T newRoot = nodeFactory.newUniqueNode(order, false, true);
            swapRoot(newRoot);
            UniqueBTreeUtils.put(root, key, left, right);
            newRoot.markChanged();
        } else {
            T parent = ancestorStack.pop();
            parent.markChanged();
            //check if parent overflows
            if (parent.getNumKeys() < order - 1) {
                UniqueBTreeUtils.put(parent, key, right);
            } else {
                Pair<T, Long > pair = UniqueBTreeUtils.putAndSplit(parent, key, right);
                T newNode = pair.getA();
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
		Pair<LinkedList<T>,T> pair = searchNodeWithHistory(key);
        T leaf = pair.getB();
        LinkedList<T> ancestorStack = pair.getA();
        deleteFromLeaf(leaf, key);
        leaf.markChanged();

        if (leaf.isRoot()) {
            return;
        }
        long replacementKey = leaf.getSmallestKey();
        T current = leaf;
        T parent = (ancestorStack.size() == 0) ? null : ancestorStack.pop();
        while (current != null && current.isUnderfull()) {
            //check if can borrow 1 value from the left or right siblings
            T rightSibling = current.rightSibling(parent);
            T leftSibling = current.leftSibling(parent);
            if (leftSibling != null && leftSibling.hasExtraKeys()) {
                BTreeUtils.redistributeKeysFromLeft(current, leftSibling, parent);
                leftSibling.markChanged();
            } else if (rightSibling != null && rightSibling.hasExtraKeys()) {
                BTreeUtils.redistributeKeysFromRight(current, rightSibling, parent);
                rightSibling.markChanged();
            } else {
                //at this point, both left and right sibling have the minimum number of keys
                if (leftSibling!= null) {
                    //merge with left sibling
                    parent = BTreeUtils.mergeWithLeft(this, current, leftSibling, parent);
                    leftSibling.markChanged();
                } else {
                    //merge with right sibling
                    parent = BTreeUtils.mergeWithRight(this, current, rightSibling, parent);
                    rightSibling.markChanged();
                }
            }
            if (UniqueBTreeUtils.containsKey(current, key)) {
                UniqueBTreeUtils.replaceKey(current, key, replacementKey);
            }
            current = parent;
            parent = (ancestorStack.size() == 0 ) ? null : ancestorStack.pop();
        }
	}

    protected Pair<LinkedList<T>, T> searchNodeWithHistory(long key) {
        LinkedList<T> stack = new LinkedList<>();
        T current = root;
        while (!current.isLeaf()) {
            stack.push(current);
            current = UniqueBTreeUtils.findChild(current, key);
        }
        return new Pair<>(stack, current);
    }

    private void deleteFromLeaf(T leaf, long key) {
        UniqueBTreeUtils.delete(leaf, key);
    }
}
