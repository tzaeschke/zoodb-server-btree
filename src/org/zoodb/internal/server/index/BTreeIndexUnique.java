package org.zoodb.internal.server.index;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

import java.util.NoSuchElementException;

public class BTreeIndexUnique extends BTreeIndex<UniquePagedBTree, UniquePagedBTreeNode> implements LongLongUIndex  {

    private UniquePagedBTree tree;
    
    public BTreeIndexUnique(StorageChannel file, boolean isNew) {
    	super(file, isNew, true);

        final int leafOrder = bufferManager.getLeafOrder();
        final int innerOrder = bufferManager.getInnerNodeOrder();
		tree = new UniquePagedBTree(file.getPageSize(), bufferManager);
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
		if (getTree().search(key) != null) {
            return false;
        }
        tree.insert(key, value);
        return true;
	}
    
    @Override
	public void clear() {
		tree = new UniquePagedBTree(tree.getPageSize(), new BTreeStorageBufferManager(file, isUnique()));
	}

	@Override
	public UniquePagedBTree getTree() {
		return tree;
	}

	@Override
	public long removeLongNoFail(long key, long failValue) {
		try {
			return removeLong(key);
		} catch (NoSuchElementException e) {
			return failValue;
		}
	}

	@Override
	public long deleteAndCheckRangeEmpty(long pos, long min, long max) {
		long ret = removeLong(pos);
		LongLongIterator<LLEntry> it = iterator(min, max);
		if(!it.hasNext()) {
			file.reportFreePage(BitTools.getPage(pos));
		}

		return ret;
	}

}
