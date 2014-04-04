package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.server.index.btree.BTreeUtils;

public class NonUniqueBTree<T extends BTreeNode> extends BTree<T> {

    public NonUniqueBTree(int order, BTreeNodeFactory nodeFactory) {
        super(order, nodeFactory);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    public boolean contains(long key, long value) {
        T current = root;
        while (!current.isLeaf()) {
            current = BTreeUtils.findChild(current, key, value);
        }
        return BTreeUtils.containsKeyValue(current, key, value);
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
    public void delete(long key, long value) {
        deleteEntry(key, value);
    }

}
