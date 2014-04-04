package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

/**
 * Utility class that contains methods that can be applied to all types of nodes.
 *
 * TODO This could be moved to the tree class.
 */
public class BTreeUtils {

    public static <T extends BTreeNode> T findChild(T node, long key, long value) {
        return (T) node.getChild(findKeyValuePos(node, key, value));
    }

    public static <T extends BTreeNode> boolean containsKeyValue(T node, long key, long value) {
        Pair<Boolean, Integer> result = node.binarySearch(key, value);
        return result.getA();
    }

    public static <T extends BTreeNode> int findKeyValuePos(T node, long key, long value) {
        if (node.getNumKeys() == 0) {
            return 0;
        }
        Pair<Boolean, Integer> result = node.binarySearch(key, value);
        int closest = result.getB();
        boolean found = result.getA();

        // if the key is not here, find the child subtree that has it
        if (!found) {
            if (closest == 0 && key < node.getKey(0)) {
                return 0;
            } else if (key < node.getKey(closest)) {
                return closest;
            }
        }
        return closest + 1;
    }

    /**
     * Inner-node put. Places key to the left of the next bigger key k'.
     *
     * Requires that key <= keys(newUniqueNode) all elements of the left child of k'
     * are smaller than key node is not full. Assumes that leftOf(key') <=
     * keys(newUniqueNode)
     *
     * @param key
     * @param newNode
     */
    public static <T extends BTreeNode> void put(T node, long key, long value, BTreeNode newNode) {
        if (node.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on inner nodes.");
        } else if (node.getNumKeys() == 0) {
            throw new IllegalStateException(
                    "Should only be called when node is non-empty.");
        }
        int pos = findKeyValuePos(node, key, value);
        if (pos > 0 && (node.getKey(pos - 1) == key && node.getValue(pos - 1) == value)) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique values.");
        }
        int recordsToMove = node.getNumKeys() - pos;
        node.shiftChildren(pos + 1, pos + 2, recordsToMove);
        node.setChild(pos + 1, newNode);

        node.shiftKeys(pos, pos + 1, recordsToMove);
        node.setEntry(pos, key, value);
        node.incrementNumKyes();
    }

    /**
     * Root-node put.
     *
     * Used when a non-leaf root is empty and will be populated by a single key
     * and two nodes.
     *
     * @param key
     *            The new key on the root.
     * @param left
     *            The left node.
     * @param right
     *            The right node.
     */
    public static <T extends BTreeNode> void put(T root, long key, long value,  T left, T right) {
        if (!root.isRoot()) {
            throw new IllegalStateException(
                    "Should only be called on the root node.");
        }
        root.setEntry(0, key, value);
        root.setNumKeys(1);

        root.setChild(0, left);
        root.setChild(1, right);
    }

    /**
     * Delete the key from the node.
     *
     * @param key
     */
    public static <T extends BTreeNode> void delete(T node, long key, long value) {
        if (!node.isLeaf()) {
            throw new IllegalStateException("Should be a leaf node");
        }
        final int keyPos = findKeyValuePos(node, key, value);
        int recordsToMove = node.getNumKeys() - keyPos;
        node.shiftRecords(keyPos, keyPos - 1, recordsToMove);
        node.decrementNumKeys();
    }


    /**
     * Puts a new key into the node and splits accordingly. Returns the newly
     * created leaf, which is to the right.
     *
     * @param newKey
     * @return
     */
    public static <T extends BTreeNode> T putAndSplit(T current, long newKey, long value) {
        if (!current.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }
        int order = current.getOrder();
        int numKeys = current.getNumKeys();
        T tempNode = (T) current.newNode(order + 1, true, false);
        current.copyFromNodeToNode(0, 0, tempNode, 0, 0, numKeys, order);
        tempNode.setNumKeys(numKeys);
        tempNode.put(newKey, value);

        int keysInLeftNode = (int) Math.ceil((order) / 2.0);
        int keysInRightNode = order - keysInLeftNode;

        // populate left node
        tempNode.copyFromNodeToNode(0, 0, current, 0, 0, keysInLeftNode, keysInLeftNode + 1);
        current.setNumKeys(keysInLeftNode);

        // populate right node
        T right = (T) current.newNode(order, true, false);
        tempNode.copyFromNodeToNode(keysInLeftNode, keysInLeftNode, right,
                0, 0, keysInRightNode, keysInRightNode + 1);
        right.setNumKeys(keysInRightNode);
        tempNode.close();

        return right;
    }

    /**
     * Puts a key and a new node to the inner structure of the tree.
     *
     * @param key
     * @param newNode
     * @return
     */
    public static <T extends BTreeNode> Pair<T, Pair<Long, Long> > putAndSplit(T current, long key, long value, T newNode) {
        if (current.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on inner nodes.");
        }
        int order = current.getOrder();
        int numKeys = current.getNumKeys();
        // create a temporary node to allow the insertion
        T tempNode = (T) current.newNode(order + 1, false, true);
        current.copyFromNodeToNode(0, 0, tempNode, 0, 0, numKeys, order);
        tempNode.setNumKeys(numKeys);
        put(tempNode, key, value, newNode);

        // split
        T right = (T) current.newNode(order, false, false);
        int keysInLeftNode = (int) Math.floor(order / 2.0);
        // populate left node
        tempNode.copyFromNodeToNode(0, 0, current, 0, 0, keysInLeftNode, keysInLeftNode + 1);
        current.setNumKeys(keysInLeftNode);

        // populate right node
        int keysInRightNode = order - keysInLeftNode - 1;
        tempNode.copyFromNodeToNode(keysInLeftNode + 1, keysInLeftNode + 1, right,
                0, 0, keysInRightNode, keysInRightNode + 1);
        right.setNumKeys(keysInRightNode);
        tempNode.close();

        return new Pair<>(right, tempNode.getKeyValue(keysInLeftNode));
    }

    public static <T extends BTreeNode> void replaceEntry(T node, long key, long value, long replacementKey, long replacementValue) {
        if (replacementKey < key) {
            throw new RuntimeException("Replacing " + key + " with "
                    + replacementKey + " might be illegal.");
        }
        int pos = findKeyValuePos(node, key, value);
        if (pos > -1) {
            node.setEntry(pos - 1, replacementKey, replacementValue);
        }
    }
}
