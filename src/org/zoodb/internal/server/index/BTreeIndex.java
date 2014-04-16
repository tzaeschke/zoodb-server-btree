package org.zoodb.internal.server.index;

import java.util.List;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.*;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;


public class BTreeIndex extends AbstractIndex implements LongLongUIndex {
	
	private UniquePagedBTree tree;
    private BTreeStorageBufferManager bufferManager;
    private boolean isUnique = true;
    
	public BTreeIndex(StorageChannel file, boolean isNew, boolean isUnique) {
		super(file, isNew, isUnique);
		
        bufferManager = new BTreeStorageBufferManager(file, isUnique);
        final int leafOrder = bufferManager.getLeafOrder();
        final int innerOrder = bufferManager.getInnerNodeOrder();
		tree = new UniquePagedBTree(innerOrder, leafOrder, bufferManager);
	}
	
	public BTreeIndex(StorageChannel file, boolean isNew, boolean isUnique, int rootPageId) {
		this(file,isNew,isUnique);

		PagedBTreeNode root = bufferManager.read(rootPageId);
		root.setIsRoot(true);
		tree.setRoot(root);
	}
	
	@Override
	public void insertLong(long key, long value) {
		tree.insert(key, value);
	}

	@Override
	public long removeLong(long key, long value) {
		return tree.delete(key);
	}

	@Override
	public void print() {
        System.out.println(tree);
	}

	@Override
	public boolean insertLongIfNotSet(long key, long value) {
		if (tree.search(key) != -1) {
            return false;
        }
        tree.insert(key, value);
        return true;
	}

	@Override
	public int statsGetLeavesN() {
		return tree.statsGetLeavesN();
	}

	@Override
	public int statsGetInnerN() {
		return tree.statsGetInnerN();
	}

	@Override
	public void clear() {
		tree = new UniquePagedBTree(tree.getInnerNodeOrder(), tree.getLeafOrder(), new BTreeStorageBufferManager(file, isUnique));

	}

	@Override
	public LongLongIterator<LLEntry> iterator() {
		return new AscendingBTreeLeafEntryIterator<>(tree);
	}

	@Override
	public LongLongIterator<LLEntry> iterator(long min, long max) {
        return new AscendingBTreeLeafEntryIterator<>(tree, min, max);
	}

	@Override
	public LongLongIterator<LLEntry> descendingIterator() {
        return new DescendingBTreeLeafEntryIterator<>(tree);
	}

	@Override
	public LongLongIterator<LLEntry> descendingIterator(long max, long min) {
        return new DescendingBTreeLeafEntryIterator<>(tree, min, max);
	}

	@Override
	public long getMinKey() {
		return tree.getMinKey();
	}

	@Override
	public long getMaxKey() {
		return tree.getMaxKey();
	}

	@Override
	public void deregisterIterator(LongLongIterator<?> it) {
		tree.deregisterIterator((BTreeLeafEntryIterator) it);
	}

	@Override
	public void refreshIterators() {
        tree.refreshIterators();
	}

	@Override
	public int write() {
		return bufferManager.write(tree.getRoot());
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

	public UniquePagedBTree getTree() {
		return tree;
	}
	
    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
	}

	@Override
	public long size() {
		return tree.size();
	}

	@Override
	public DATA_TYPE getDataType() {
		return this.getDataType();
	}

	@Override
	public List<Integer> debugPageIds() {
		return bufferManager.debugPageIds(tree);
	}

	@Override
	public int statsGetWrittenPagesN() {
		return bufferManager.getStatNWrittenPages();
	}

	@Override
	public long removeLongNoFail(long key, long failValue) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long deleteAndCheckRangeEmpty(long pos, long min, long max) {
		// TODO Auto-generated method stub
		return 0;
	}
}
