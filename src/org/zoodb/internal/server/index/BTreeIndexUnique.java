package org.zoodb.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

public class BTreeIndexUnique extends BTreeIndex<UniquePagedBTree, UniquePagedBTreeNode> implements LongLongUIndex  {

    private UniquePagedBTree tree;
    
    public BTreeIndexUnique(DATA_TYPE dataType, StorageChannel file) {
    	super(dataType, file, true, true);
		tree = new UniquePagedBTree(bufferManager.getPageSize(), bufferManager);
        setEmptyRoot();
    }

	public BTreeIndexUnique(DATA_TYPE dataType, StorageChannel file, int rootPageId) {
		this(dataType, file);
        readAndSetRoot(rootPageId);
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
		tree = new UniquePagedBTree(tree.getInnerNodeOrder(), tree.getLeafOrder(), new BTreeStorageBufferManager(file, isUnique()));
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
