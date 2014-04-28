package org.zoodb.internal.server.index;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;

public class BTreeIndexNonUnique extends BTreeIndex<NonUniquePagedBTree, NonUniquePagedBTreeNode> implements LongLongIndex {

    private NonUniquePagedBTree tree;

    public BTreeIndexNonUnique(StorageChannel file, boolean isNew) {
        super(file, isNew, false);

        final int leafOrder = bufferManager.getLeafOrder();
        final int innerOrder = bufferManager.getInnerNodeOrder();
        tree = new NonUniquePagedBTree(innerOrder, leafOrder, bufferManager);
    }

    @Override
    public boolean insertLongIfNotSet(long key, long value) {
        if (tree.contains(key, value)) {
            return false;
        }
        tree.insert(key, value);
        return true;
    }
    
    @Override
	public long removeLong(long key, long value) {
		return tree.delete(key, value);
	}

    @Override
    public void clear() {
		tree = new NonUniquePagedBTree(tree.getInnerNodeOrder(), tree.getLeafOrder(), new BTreeStorageBufferManager(file, isUnique()));
    }

	public NonUniquePagedBTree getTree() {
		return tree;
	}

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
    }

}
