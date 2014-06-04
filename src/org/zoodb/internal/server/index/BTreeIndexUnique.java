package org.zoodb.internal.server.index;

import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

public class BTreeIndexUnique extends BTreeIndex implements LongLongUIndex  {

    private UniquePagedBTree tree;
    
    public BTreeIndexUnique(DiskIO.DATA_TYPE dataType, StorageChannel file) {
    	super(dataType, file, true, true);
    	initTree();
    }
    
    public BTreeIndexUnique(DiskIO.DATA_TYPE dataType, int nodeValueSizeInByte, StorageChannel file) {
    	super(dataType, file, true, true);
    	bufferManager.setNodeValueElementSize(nodeValueSizeInByte);
    	initTree();
    }
    
	public BTreeIndexUnique(DiskIO.DATA_TYPE dataType, StorageChannel file, int rootPageId) {
    	super(dataType, file, true, true);
    	loadTree(rootPageId);

    }
	
	public BTreeIndexUnique(DiskIO.DATA_TYPE dataType, int nodeValueSizeInByte, StorageChannel file, int rootPageId) {
    	super(dataType, file, true, true);
    	bufferManager.setNodeValueElementSize(nodeValueSizeInByte);
    	loadTree(rootPageId);
    }
	
	public void initTree() {
		tree = new UniquePagedBTree(bufferManager.getPageSize(), bufferManager);
	}
	
	public void loadTree(int rootPageId) {
        UniquePagedBTreeNode root = (UniquePagedBTreeNode)bufferManager.read(rootPageId);
        root.setIsRoot(true);
		tree = new UniquePagedBTree(root, bufferManager.getPageSize(), bufferManager);
	}

    @Override
	public LLEntry findValue(long key) {
		Long value = tree.search(key);
		if (value != null) { 
            return new LLEntry(key, value);
		} else { 
			return null;
		}
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
        return tree.insert(key, value, true);
	}
    
    @Override
	public void clear() {
    	bufferManager.clear(tree.getRoot());
		tree = new UniquePagedBTree(tree.getPageSize(), bufferManager);
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
