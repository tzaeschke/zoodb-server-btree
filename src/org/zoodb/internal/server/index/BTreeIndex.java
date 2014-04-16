package org.zoodb.internal.server.index;

import java.util.List;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.btree.AscendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;


public abstract class BTreeIndex<T extends PagedBTree<U>, U extends PagedBTreeNode> extends AbstractIndex {
	
    protected BTreeStorageBufferManager bufferManager;
    
	public BTreeIndex(StorageChannel file, boolean isNew, boolean isUnique) {
		super(file, isNew, isUnique);
		
        bufferManager = new BTreeStorageBufferManager(file, isUnique);

	}
	
	public BTreeIndex(StorageChannel file, boolean isNew, boolean isUnique, int rootPageId) {
		this(file,isNew,isUnique);

		PagedBTreeNode root = bufferManager.read(rootPageId);
		root.setIsRoot(true);
		this.getTree().setRoot(root);
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

	public LongLongIterator<LLEntry> iterator() {
		return new AscendingBTreeLeafEntryIterator<>(getTree());
	}

	public LongLongIterator<LLEntry> iterator(long min, long max) {
        return new AscendingBTreeLeafEntryIterator<>(getTree(), min, max);
	}

	public LongLongIterator<LLEntry> descendingIterator() {
        return new DescendingBTreeLeafEntryIterator<>(getTree());
	}

	public LongLongIterator<LLEntry> descendingIterator(long max, long min) {
        return new DescendingBTreeLeafEntryIterator<>(getTree(), min, max);
	}

	public long getMinKey() {
		return getTree().getMinKey();
	}

	public long getMaxKey() {
		return getTree().getMaxKey();
	}

	@SuppressWarnings("unchecked")
	public void deregisterIterator(LongLongIterator<?> it) {
		getTree().deregisterIterator((BTreeLeafEntryIterator<U>) it);
	}

	public void refreshIterators() {
        getTree().refreshIterators();
	}

	public int write() {
		return bufferManager.write(getTree().getRoot());
	}

	public abstract T getTree();
	
    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
	}

	public long size() {
		return getTree().size();
	}

	public DATA_TYPE getDataType() {
		return this.getDataType();
	}

	public List<Integer> debugPageIds() {
		return bufferManager.debugPageIds(getTree());
	}

	public int statsGetWrittenPagesN() {
		return bufferManager.getStatNWrittenPages();
	}

	public long removeLongNoFail(long key, long failValue) {
		// TODO Auto-generated method stub
		return 0;
	}

	public long deleteAndCheckRangeEmpty(long pos, long min, long max) {
		// TODO Auto-generated method stub
		return 0;
	}
}
