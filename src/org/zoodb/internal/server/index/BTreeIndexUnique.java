package org.zoodb.internal.server.index;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

public class BTreeIndexUnique extends BTreeIndex<UniquePagedBTree, UniquePagedBTreeNode> implements LongLongUIndex  {

    private UniquePagedBTree tree;
    
    public BTreeIndexUnique(StorageChannel file, boolean isNew, boolean isUnique) {
    	super(file, isNew, isUnique);
    	
        final int leafOrder = bufferManager.getLeafOrder();
        final int innerOrder = bufferManager.getInnerNodeOrder();
		tree = new UniquePagedBTree(innerOrder, leafOrder, bufferManager);
    }

	public BTreeIndexUnique(StorageChannel file, boolean isNew, boolean isUnique, int rootPageId) {
		super(file, isUnique, isNew, rootPageId);
	}
	
    @Override
	public LLEntry findValue(long key) {
		Long value = tree.search(key);
		if(value != null) 
            return new LLEntry(key, value);
		else 
			return null;
	}

	@Override
	public long removeLong(long key) {
		return tree.delete(key);
	}
	
	@Override
    public long removeLong(long key, long value) {
		return getTree().delete(key);
	}

    @Override
	public boolean insertLongIfNotSet(long key, long value) {
		if (getTree().search(key) != -1) {
            return false;
        }
        tree.insert(key, value);
        return true;
	}
    
    @Override
	public void clear() {
		tree = new UniquePagedBTree(tree.getInnerNodeOrder(), tree.getLeafOrder(), new BTreeStorageBufferManager(file, isUnique()));
	}

	@Override
	public UniquePagedBTree getTree() {
		return tree;
	}

}
