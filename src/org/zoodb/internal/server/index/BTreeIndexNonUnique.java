package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;

public class BTreeIndexNonUnique extends BTreeIndex<NonUniquePagedBTree, NonUniquePagedBTreeNode> implements LongLongIndex {

    private NonUniquePagedBTree tree;

    public BTreeIndexNonUnique(PAGE_TYPE dataType, IOResourceProvider file) {
        super(dataType, file, true, false);

        tree = new NonUniquePagedBTree(bufferManager.getInnerNodeOrder(), 
        						bufferManager.getLeafOrder(), bufferManager);
    }
    
    public BTreeIndexNonUnique(PAGE_TYPE dataType, IOResourceProvider file, int rootPageId) {
        super(dataType, file, true, false);
        
        NonUniquePagedBTreeNode root = (NonUniquePagedBTreeNode)bufferManager.read(rootPageId);
        root.setIsRoot(true);
        
        tree = new NonUniquePagedBTree(root, bufferManager.getInnerNodeOrder(), 
        						bufferManager.getLeafOrder(), bufferManager);
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
