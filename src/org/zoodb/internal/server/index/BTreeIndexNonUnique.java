package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;

public class BTreeIndexNonUnique extends BTreeIndex<NonUniquePagedBTree, NonUniquePagedBTreeNode> implements LongLongIndex {

    private NonUniquePagedBTree tree;

    public BTreeIndexNonUnique(DiskIO.DATA_TYPE dataType, StorageChannel file) {
        super(dataType, file, true, false);

        tree = new NonUniquePagedBTree(bufferManager.getPageSize(), bufferManager);
        setEmptyRoot();
    }
    
    public BTreeIndexNonUnique(DiskIO.DATA_TYPE dataType, StorageChannel file, int rootPageId) {
        this(dataType, file);
        readAndSetRoot(rootPageId);
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
		tree = new NonUniquePagedBTree(tree.getPageSize(), new BTreeStorageBufferManager(file, isUnique()));
    }

	public NonUniquePagedBTree getTree() {
		return tree;
	}

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
    }

}
