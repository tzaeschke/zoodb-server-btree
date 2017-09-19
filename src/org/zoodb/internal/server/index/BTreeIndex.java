package org.zoodb.internal.server.index;

import java.util.List;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.server.index.btree.AscendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;


public abstract class BTreeIndex<T extends PagedBTree<U>, U extends PagedBTreeNode> extends AbstractIndex {
	
    protected BTreeStorageBufferManager bufferManager;
    protected PAGE_TYPE dataType;
    
	public BTreeIndex(PAGE_TYPE dataType, IOResourceProvider file, boolean isNew, boolean isUnique) {
		super(file, isNew, isUnique);
        bufferManager = new BTreeStorageBufferManager(file, isUnique, dataType);
        this.dataType = dataType;
	}
	
	public void insertLong(long key, long value) {
		getTree().insert(key, value);
	}

	public void print() {
        System.out.println(getTree());
	}
	
	public int statsGetLeavesN() {
		return getTree().statsGetLeavesN();
	}

	public int statsGetInnerN() {
		return getTree().statsGetInnerN();
	}

	public LLEntryIterator iterator() {
		return new AscendingBTreeLeafEntryIterator<>(getTree());
	}

	public LLEntryIterator iterator(long min, long max) {
        return new AscendingBTreeLeafEntryIterator<>(getTree(), min, max);
	}

	public LLEntryIterator descendingIterator() {
        return new DescendingBTreeLeafEntryIterator<>(getTree());
	}

	public LLEntryIterator descendingIterator(long max, long min) {
        return new DescendingBTreeLeafEntryIterator<>(getTree(), min, max);
	}

	public long getMinKey() {
		return getTree().getMinKey();
	}

	public long getMaxKey() {
		return getTree().getMaxKey();
	}

	public int write(StorageChannelOutput out) {
		return bufferManager.write(getTree().getRoot(), out);
	}

	public long size() {
		return getTree().size();
	}

	public PAGE_TYPE getDataType() {
		return dataType;
	}
	
    public List<Integer> debugPageIds() {
		return bufferManager.debugPageIds(getTree());
	}

	public int statsGetWrittenPagesN() {
		return bufferManager.getStatNWrittenPages();
	}
	
    public abstract T getTree();
	
    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
	}
}
