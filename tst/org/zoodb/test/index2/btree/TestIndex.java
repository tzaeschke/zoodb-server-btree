package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndexNonUnique;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.IndexFactory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
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
	
    @Test
    public void testLoadedPagesNotDirty() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = ind.write();

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf, root);
        int w1 = ind2.statsGetWrittenPagesN();
        Iterator<LongLongIndex.LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        int n = 0;
        while (i.hasNext()) {
        	n++;
        	i.next();
        }
        ind2.write();
        int w2 = ind2.statsGetWrittenPagesN();
        //no pages written on freshly read root
        assertEquals("w1=" + w1, 0, w1);
        //no pages written when only reading
        assertEquals("w1=" + w1 + "  w2=" + w2, w1, w2);
                
        //now add one element and see how much gets written
//        ind2.insertLong(-1, -1);
//        assertNotNull(ind2.findValue(-1));
//        ind2.insertLong(11, 11);
        ind2.insertLong(1100, 1100);
//        LLEntry e = ind2.findValue(1100);
//        assertNotNull(e);
//        assertEquals(1100, e.getValue());
        ind2.write();
        int wn = ind2.statsGetWrittenPagesN();
//        System.out.println("w2=" + w2);
//        System.out.println("wn=" + wn);
        assertTrue("wn=" + wn, wn > w2);
        
//        assertEquals(MAX, n);
    }

	public static void findAll(LongLongIndex index, List<LLEntry> list) {
		for (LLEntry entry : list) {
			LongLongIterator<LLEntry> it = index.iterator(entry.getKey(), entry.getKey());
			assertEquals(it.next().getValue(), entry.getValue());
		}
	}

}
