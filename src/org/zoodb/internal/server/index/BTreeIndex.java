package org.zoodb.internal.server.index;

import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.btree.BTree;


public class BTreeIndex extends AbstractIndex implements LongLongUIndex {
	
	private BTree tree;

	public BTreeIndex(StorageChannel file, boolean isNew, boolean isUnique) {
		super(file, isNew, isUnique);
		
		// TODO Auto-generated constructor stub
		final int order = 4;
		tree = new BTree(order);
	}

	@Override
	public void insertLong(long key, long value) {
		// TODO Auto-generated method stub

	}

	@Override
	public long removeLong(long key, long value) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void print() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean insertLongIfNotSet(long key, long value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int statsGetLeavesN() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int statsGetInnerN() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public LongLongIterator<LLEntry> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LongLongIterator<LLEntry> iterator(long min, long max) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LongLongIterator<LLEntry> descendingIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LongLongIterator<LLEntry> descendingIterator(long max, long min) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getMinKey() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getMaxKey() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void deregisterIterator(LongLongIterator<?> it) {
		// TODO Auto-generated method stub

	}

	@Override
	public void refreshIterators() {
		// TODO Auto-generated method stub

	}

	@Override
	public int write() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public LLEntry findValue(long key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long removeLong(long key) {
		// TODO Auto-generated method stub
		return 0;
	}

}
