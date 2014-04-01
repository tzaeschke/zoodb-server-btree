package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.util.Pair;

/**
 * Corresponds to Unique B+ tree indices.
 */
public class UniquePagedBTreeNode extends PagedBTreeNode {

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
        super(bufferManager, order, isLeaf, isRoot);
    }

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, order, isLeaf, isRoot, pageId);
    }

    @Override
    public UniquePagedBTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
        return new UniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
    }

    /**
     * Returns the index + 1 of the key received as an argument. If the key is
     * not in the array, it will return the index of the smallest key in the
     * array that is larger than the key received as argument.
     *
     * @param key
     * @return
     */
    public int findKeyPos(long key) {
        // ToDo compare keys and values
        if (getNumKeys() == 0) {
            return 0;
        }
        Pair<Boolean, Integer> result = binarySearch(key);
        int closest = result.getB();
        boolean found = result.getA();

        // if the key is not here, find the child subtree that has it
        if (!found) {
            if (closest == 0 && key < getKey(0)) {
                return 0;
            } else if (key < getKey(closest)) {
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
    public long findValue(long key) {
        if (!this.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }
        if (getNumKeys() == 0) {
            return -1;
        }
        Pair<Boolean, Integer> result = binarySearch(key);
        int position = result.getB();
        boolean found = result.getA();

        return found ? getValue(position) : -1;
    }

    /**
     * Check if this node contains the key received as argument.
     * @param key
     * @return          True if the node contains the key, false otherwise.
     */
    public boolean containsKey(long key) {
        if (getNumKeys() == 0) {
            return false;
        }
        Pair<Boolean, Integer> result = binarySearch(key);
        boolean found = result.getA();
        return found;
    }

    /**
     * Find the child subtree that contains the leaf node corresponding to the key received as argument.
     * @param key
     * @return          The child of the current node that contains the key or the root of a subtree that contains the key.
     */
    public UniquePagedBTreeNode findChild(long key) {
        return (UniquePagedBTreeNode) getChild(findKeyPos(key));
    }

    /**
     * Leaf put.
     *
     * Requires that node is not full.
     *
     * @param key
     * @param value
     */
    public void put(long key, long value) {
        if (!isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }

        int pos = findKeyPos(key);
        if (pos > numKeys && getKey(pos) == key) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique keys.");
        }
        shiftRecords(pos, pos + 1, getNumKeys() - pos);
        setKey(pos, key);
        setValue(pos, value);
        incrementNumKyes();
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
    public void put(long key, BTreeNode newNode) {
        if (isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on inner nodes.");
        } else if (getNumKeys() == 0) {
            throw new IllegalStateException(
                    "Should only be called when node is non-empty.");
        }

        int pos = findKeyPos(key);
        if (pos > numKeys && getKey(pos) == key) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique keys.");
        }
        int recordsToMove = getNumKeys() - pos;
        shiftChildren(pos + 1, pos + 2, recordsToMove);
        setChild(pos + 1, newNode);

        shiftKeys(pos, pos + 1, recordsToMove);
        setKey(pos, key);
        incrementNumKyes();
    }

    /**
     * Delete the key from the node.
     *
     * @param key
     */
    public void delete(long key) {
        if (!isLeaf()) {
            throw new IllegalStateException("Should be a leaf node");
        }
        final int keyPos = findKeyPos(key);
        int recordsToMove = getNumKeys() - keyPos;
        shiftRecords(keyPos, keyPos - 1, recordsToMove);
        decrementNumKeys();
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
    protected Pair<Boolean, Integer> binarySearch(long key) {
        int low = 0;
        int high = getNumKeys() - 1;
        int mid = 0;
        boolean found = false;
        while (!found && low <= high) {
            mid = low + (high - low) / 2;
            if (getKey(mid) == key) {
                found = true;
            } else {
                if (key < getKey(mid)) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
        }
        return new Pair<>(found, mid);
    }
}
