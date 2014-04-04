package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.util.Pair;

/**
 * B+ Tree data structure.
 *
 * The order parameter is considered according the Knuth, as the
 * maximum number of children in any node.
 *
 * Proper support for handling duplicate keys needs to be added.
 */
public abstract class UniqueBTree<T extends BTreeNode> extends BTree<T> {

    private static final int NO_VALUE = 0;

    public UniqueBTree(int order, BTreeNodeFactory nodeFactory) {
        super(order, nodeFactory);
    }

    @Override
    public boolean isUnique() {
        return true;
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
            current = (T) current.findChild(key, NO_VALUE);
        }
        return findValue(current, key);
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
		deleteEntry(key, NO_VALUE);
	}

    private long findValue(T node, long key) {
        if (!node.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }
        if (node.getNumKeys() == 0) {
            return -1;
        }
        Pair<Boolean, Integer> result = node.binarySearch(key, NO_VALUE);
        int position = result.getB();
        boolean found = result.getA();

        return found ? node.getValue(position) : -1;
    }
}
