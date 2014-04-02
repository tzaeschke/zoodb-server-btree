package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.util.Pair;

public class UniqueBTreeUtils {



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
    public static <T extends BTreeNode> Pair<T, Long> putAndSplit(T current, long key, T newNode) {
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
        put(tempNode, key, newNode);

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

    public static <T extends BTreeNode> void replaceKey(T node, long key, long replacementKey) {
        if (replacementKey < key) {
            throw new RuntimeException("Replacing " + key + " with "
                    + replacementKey + " might be illegal.");
        }
        int pos = findKeyPos(node, key);
        if (pos > -1) {
            node.setKey(pos - 1, replacementKey);
        }
    }

    /**
     * Returns the index + 1 of the key received as an argument. If the key is
     * not in the array, it will return the index of the smallest key in the
     * array that is larger than the key received as argument.
     *
     * @param key
     * @return
     */
    public static <T extends BTreeNode> int findKeyPos(T node, long key) {
        // ToDo compare keys and values
        if (node.getNumKeys() == 0) {
            return 0;
        }
        Pair<Boolean, Integer> result = binarySearch(node, key);
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
     * Find the value corresponding to a key in a leaf node.
     *
     * @param key
     *            The key received as argument
     * @return The value corresponding to the key in the index. If the key is
     *         not found in the index, -1 is returned.
     */
    public static <T extends BTreeNode> long findValue(T node, long key) {
        if (!node.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }
        if (node.getNumKeys() == 0) {
            return -1;
        }
        Pair<Boolean, Integer> result = binarySearch(node, key);
        int position = result.getB();
        boolean found = result.getA();

        return found ? node.getValue(position) : -1;
    }

    /**
     * Check if this node contains the key received as argument.
     * @param key
     * @return          True if the node contains the key, false otherwise.
     */
    public static <T extends BTreeNode> boolean containsKey(T node, long key) {
        if (node.getNumKeys() == 0) {
            return false;
        }
        Pair<Boolean, Integer> result = binarySearch(node, key);
        boolean found = result.getA();
        return found;
    }

    /**
     * Find the child subtree that contains the leaf node corresponding to the key received as argument.
     * @param key
     * @return          The child of the current node that contains the key or the root of a subtree that contains the key.
     */
    public static <T extends BTreeNode> T findChild(T node, long key) {
        return (T) node.getChild(findKeyPos(node, key));
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

        int pos = findKeyPos(node, key);
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
    public static <T extends BTreeNode> void put(T node, long key, BTreeNode newNode) {
        if (node.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on inner nodes.");
        } else if (node.getNumKeys() == 0) {
            throw new IllegalStateException(
                    "Should only be called when node is non-empty.");
        }

        int pos = findKeyPos(node, key);
        if (pos > node.getNumKeys() && node.getKey(pos) == key) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique keys.");
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
    public static <T extends BTreeNode> void delete(T node, long key) {
        if (!node.isLeaf()) {
            throw new IllegalStateException("Should be a leaf node");
        }
        final int keyPos = findKeyPos(node, key);
        int recordsToMove = node.getNumKeys() - keyPos;
        node.shiftRecords(keyPos, keyPos - 1, recordsToMove);
        node.decrementNumKeys();
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
    public static <T extends BTreeNode> Pair<Boolean, Integer> binarySearch(T node, long key) {
        int low = 0;
        int high = node.getNumKeys() - 1;
        int mid = 0;
        boolean found = false;
        while (!found && low <= high) {
            mid = low + (high - low) / 2;
            if (node.getKey(mid) == key) {
                found = true;
            } else {
                if (key < node.getKey(mid)) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
        }
        return new Pair<>(found, mid);
    }
}
