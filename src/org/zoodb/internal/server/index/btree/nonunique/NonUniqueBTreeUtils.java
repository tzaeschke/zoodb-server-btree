package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.util.Pair;

public class NonUniqueBTreeUtils {

    public static <T extends BTreeNode> T findChild(T node, long key, long value) {
        return (T) node.getChild(findKeyValuePos(node, key, value));
    }

    private static <T extends BTreeNode> int findKeyValuePos(T node, long key, long value) {
        if (node.getNumKeys() == 0) {
            return 0;
        }
        Pair<Boolean, Integer> result = binarySearch(node, key, value);
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
     * Leaf put.
     *
     * Requires that node is not full.
     *
     * @param key
     * @param value
     */
    public static <T extends BTreeNode> void put(T node, long key, long value) {
        if (!node.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }

        int pos = findKeyValuePos(node, key, value);
        if (pos > node.getNumKeys() && node.getKey(pos) == key) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique keys.");
        }
        node.shiftRecords(pos, pos + 1, node.getNumKeys() - pos);
        node.setKey(pos, key);
        node.setValue(pos, value);
        node.incrementNumKyes();
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
        if (pos > node.getNumKeys() && (node.getKey(pos) == key && node.getValue(pos) == value)) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique values.");
        }
        int recordsToMove = node.getNumKeys() - pos;
        node.shiftChildren(pos + 1, pos + 2, recordsToMove);
        node.setChild(pos + 1, newNode);

        node.shiftKeys(pos, pos + 1, recordsToMove);
        node.setKey(pos, key);
        node.incrementNumKyes();
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
        put(tempNode, newKey, value);

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

        return right;
    }

    /**
     * Puts a key and a new node to the inner structure of the tree.
     *
     * @param key
     * @param newNode
     * @return
     */
    public static <T extends BTreeNode> Pair<T, Long> putAndSplit(T current, long key, long value, T newNode) {
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

        long keyToMoveUp = tempNode.getKeys()[keysInLeftNode];

        return new Pair<>(right, keyToMoveUp);
    }

    /**
     * Perform binary search on the key array for a certain key
     *
     *
     * @param key   The key received as an argument.
     * @return      In case the key is contained in the key array,
     *              returns the position of the key in this array.
     *              If the key is not found, returns -1.
     */
    public static <T extends BTreeNode> Pair<Boolean, Integer> binarySearch(T node, long key, long value) {
        int low = 0;
        int high = node.getNumKeys() - 1;
        int mid = 0;
        boolean found = false;
        while (!found && low <= high) {
            mid = low + (high - low) / 2;
            if (containsAtPosition(node, mid, key, value)) {
                found = true;
            } else {
                if (smallerThanKeyValue(node, mid, key, value)) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
        }
        return new Pair<>(found, mid);
    }

    private static <T extends BTreeNode> boolean containsAtPosition(T node, int position, long key, long value) {
        return node.getKey(position) == key && node.getValue(position) == value;
    }

    private static <T extends BTreeNode> boolean smallerThanKeyValue(T node, int position, long key, long value) {
        if (key < node.getKey(position) ||
                (key == node.getKey(position) && value < node.getValue(position))) {
            return true;
        }
        return false;
    }
}
