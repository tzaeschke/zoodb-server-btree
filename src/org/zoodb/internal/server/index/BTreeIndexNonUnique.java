package org.zoodb.internal.server.index;

import java.util.List;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.AscendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.nonunique.NonUniqueBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;

public class BTreeIndexNonUnique extends AbstractIndex implements LongLongIndex {

    private NonUniquePagedBTree tree;
    private BTreeStorageBufferManager bufferManager;
    private boolean isUnique = false;

    public BTreeIndexNonUnique(StorageChannel file, boolean isNew, boolean isUnique) {
        super(file, isNew, isUnique);

        bufferManager = new BTreeStorageBufferManager(file, isUnique);
        final int leafOrder = bufferManager.getLeafOrder();
        final int innerOrder = bufferManager.getInnerNodeOrder();
        tree = new NonUniquePagedBTree(innerOrder, leafOrder, bufferManager);
    }

    @Override
    public void insertLong(long key, long value) {
        tree.insert(key, value);
    }

    @Override
    public long removeLong(long key, long value) {
        return tree.delete(key, value);
    }

    @Override
    public void print() {
        System.out.println(tree);
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
    public int statsGetLeavesN() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int statsGetInnerN() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
		tree = new NonUniquePagedBTree(tree.getInnerNodeOrder(), tree.getLeafOrder(), new BTreeStorageBufferManager(file, isUnique));
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
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getMaxKey() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
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

	public NonUniqueBTree getTree() {
		return tree;
	}

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
    }

	@Override
	public long size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DATA_TYPE getDataType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Integer> debugPageIds() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int statsGetWrittenPagesN() {
		// TODO Auto-generated method stub
		return 0;
	}
}
