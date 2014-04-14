package org.zoodb.internal.server.index;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.AscendingBTreeLeafIterator;
import org.zoodb.internal.server.index.btree.BTreeLeafIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafIterator;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;

public class NonUniqueBTreeIndex extends AbstractIndex implements LongLongIndex {

    private NonUniquePagedBTree tree;
    private BTreeStorageBufferManager bufferManager;
    private boolean isUnique = false;

    public NonUniqueBTreeIndex(StorageChannel file, boolean isNew, boolean isUnique) {
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
		return new AscendingBTreeLeafIterator<>(tree);
    }

    @Override
    public LongLongIterator<LLEntry> iterator(long min, long max) {
        return new AscendingBTreeLeafIterator<>(tree, min, max);
    }

    @Override
    public LongLongIterator<LLEntry> descendingIterator() {
        return new DescendingBTreeLeafIterator<>(tree);
    }

    @Override
    public LongLongIterator<LLEntry> descendingIterator(long max, long min) {
        return new DescendingBTreeLeafIterator<>(tree, min, max);
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
		tree.deregisterIterator((BTreeLeafIterator) it);
    }

    @Override
    public void refreshIterators() {
        tree.refreshIterators();
    }

    @Override
    public int write() {
		return bufferManager.write(tree.getRoot());
	}

	public NonUniquePagedBTree getTree() {
		return tree;
	}

    public BTreeStorageBufferManager getBufferManager() {
		return bufferManager;
    }
}
