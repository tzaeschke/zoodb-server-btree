package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;

public class BTreeIndexNonUnique extends BTreeIndex implements LongLongIndex {

    private NonUniquePagedBTree tree;

    public BTreeIndexNonUnique(DiskIO.DATA_TYPE dataType, StorageChannel file) {
        super(dataType, file, true, false);

        tree = new NonUniquePagedBTree(bufferManager.getPageSize(), bufferManager);
    }
    
    public BTreeIndexNonUnique(DiskIO.DATA_TYPE dataType, StorageChannel file, int rootPageId) {
        super(dataType, file, true, false);
        
        NonUniquePagedBTreeNode root = (NonUniquePagedBTreeNode)bufferManager.read(rootPageId);
        root.setIsRoot(true);
        
        tree = new NonUniquePagedBTree(root, bufferManager.getPageSize(), bufferManager);
    }
    
    @Override
	public long removeLong(long key, long value) {
		return tree.delete(key, value);
	}

    @Override
    public void clear() {
    	bufferManager.clear(tree.getRoot());
		tree = new NonUniquePagedBTree(tree.getPageSize(), bufferManager);
    }

	public NonUniquePagedBTree getTree() {
		return tree;
	}

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
    }

}
