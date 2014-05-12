package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndexNonUnique;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.test.index2.performance.PerformanceTest;
import org.zoodb.tools.ZooConfig;

public class TestIndex {

	private StorageChannel createPageAccessFile() {
		StorageChannel paf = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		return paf;
	}

	@Test
	public void testWriteRead() {
		StorageChannel file = createPageAccessFile();
		BTreeIndexUnique ind1 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file);

		ArrayList<LLEntry> entries = PerformanceTest.randomEntriesUnique(1000,
				42);
		PerformanceTest.insertList(ind1, entries);
		int rootPageId = ind1.write();

		BTreeIndexUnique ind2 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file, rootPageId);
		findAll(ind2, entries);
	}
	
	@Test
	public void testWriteReadEmptyUnique() {
		/* Unique */
		StorageChannel file = createPageAccessFile();
		BTreeIndexUnique ind1 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file);

		int rootPageId = ind1.write();

		BTreeIndexUnique ind2 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file, rootPageId);
		assertEquals(0, ind2.getTree().getRoot().getNumKeys());
	}
	
	@Test
	public void testWriteReadEmptyNonUnique() {
		
		/* Non-Unique */
		StorageChannel file = createPageAccessFile();
		BTreeIndexNonUnique ind1 = new BTreeIndexNonUnique(DATA_TYPE.GENERIC_INDEX,
				file);

		int rootPageId = ind1.write();

		BTreeIndexNonUnique ind2 = new BTreeIndexNonUnique(DATA_TYPE.GENERIC_INDEX,
				file, rootPageId);
		assertEquals(0, ind2.getTree().getRoot().getNumKeys());
	}

	public static void findAll(LongLongIndex index, List<LLEntry> list) {
		for (LLEntry entry : list) {
			LongLongIterator<LLEntry> it = index.iterator(entry.getKey(), entry.getKey());
			assertEquals(it.next().getValue(), entry.getValue());
		}
	}

}
