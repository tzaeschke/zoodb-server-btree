package org.zoodb.internal.server.index;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;

public class NonUniqueBTreeIndex extends AbstractIndex implements LongLongIndex {

    private NonUniquePagedBTree tree;

    public NonUniqueBTreeIndex(StorageChannel file, boolean isNew, boolean isUnique) {
        super(file, isNew, isUnique);

        final int order = 4;
        tree = new NonUniquePagedBTree(order, new BTreeStorageBufferManager(file));
    }

    @Override
    public void insertLong(long key, long value) {
        tree.insert(key, value);
    }

    @Override
    public long removeLong(long key, long value) {
        tree.delete(key, value);
        return 0;
    }

    @Override
    public void print() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean insertLongIfNotSet(long key, long value) {
        //TODO add additional method to check if the tree actually contains the entry
        tree.insert(key, value);
        return false;
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
        //To change body of implemented methods use File | Settings | File Templates.
        //TODO need to notify BufferManager of this
    }

    @Override
    public LongLongIterator<LLEntry> iterator() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongLongIterator<LLEntry> iterator(long min, long max) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongLongIterator<LLEntry> descendingIterator() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongLongIterator<LLEntry> descendingIterator(long max, long min) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void refreshIterators() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int write() {
        //TODO
        return 0;
    }
}
