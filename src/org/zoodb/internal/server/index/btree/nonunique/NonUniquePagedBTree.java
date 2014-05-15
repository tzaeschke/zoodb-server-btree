package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;

public class NonUniquePagedBTree extends PagedBTree<NonUniquePagedBTreeNode> {

	public NonUniquePagedBTree(NonUniquePagedBTreeNode root,
			int pageSize, BTreeBufferManager bufferManager) {
		super(root, pageSize, bufferManager);
	}

    public NonUniquePagedBTree(int pageSize, BTreeBufferManager bufferManager) {
        super(pageSize, bufferManager);
    }

    @Override
    public boolean isUnique() {
        return false;
    }

    public boolean contains(long key, long value) {
        PagedBTreeNode current = root;
        while (!current.isLeaf()) {
            current = current.findChild(key, value);
        }
        return current.containsKeyValue(key, value);
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
    public long delete(long key, long value) {
        return deleteEntry(key, value);
    }
}
