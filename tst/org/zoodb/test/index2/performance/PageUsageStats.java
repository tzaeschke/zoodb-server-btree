package org.zoodb.test.index2.performance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndex;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.internal.server.index.btree.BTreeLeafEntryIterator;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.ZooConfig;

public class PageUsageStats {

	private static final int PAGE_SIZE = 128;

	public static void main(String[] args) {
		ZooConfig.setFilePageSize(PAGE_SIZE);
		DBStatistics.enable(true);

		PageUsageStats stats = new PageUsageStats();
		stats.insertAndDelete();
		stats.clear();
		stats.insertAndDeleteMultiple();

		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}
	
    PagedUniqueLongLong oldIndex;
    StorageChannel oldStorage; 
    BTreeIndexUnique newIndex;
    StorageChannel newStorage;

	public PageUsageStats() {
		this.clear();
	}
	
    public void clear() {
		oldStorage = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		oldIndex = new PagedUniqueLongLong(
				DATA_TYPE.GENERIC_INDEX, oldStorage);

		newStorage = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		newIndex = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX, newStorage);
	}
			
	public void insertAndDelete() {
		System.out.println("Orders (inner:leaf), Old: " + (oldIndex.getMaxInnerN() + 1) + ":"
				+ (oldIndex.getMaxLeafN() + 1) + "\t" + "New Order: "
				+ newIndex.getTree().getInnerNodeOrder() + ":"
				+ newIndex.getTree().getLeafOrder());

		int numElements = 100000;
		ArrayList<LLEntry> entries = PerformanceTest
				.randomEntriesUnique(numElements, 42);
		
		/*
		 * Insert elements
		 */
		System.out.println("");
		System.out.println("Insert " + numElements);

		System.gc();
		System.out.println("mseconds old: " + PerformanceTest.insertList(oldIndex, entries));
		oldIndex.write();
		
		System.gc();
		System.out.println("mseconds new: " + PerformanceTest.insertList(newIndex, entries));
		newIndex.write();
		
		BTreeIterator it = new BTreeIterator(newIndex.getTree());
		int height = 1;
		while(it.hasNext()) {
			if(it.next().isLeaf()) break;
			height++;
		}
		System.out.println("Height new Index: " + height);
		
		printStats();
		
		/*
		 * Iterate
		 */
        System.out.println("");
		System.out.println("Iterate " + numElements);
		System.gc();
		System.out.println("mseconds old: " + iterate(oldIndex, 10));
		System.gc();
		System.out.println("mseconds new: " + iterate(newIndex, 10));
		
        /*
		 * Find all
		 */
        System.out.println("");
		System.out.println("Find all " + numElements);
		System.gc();
		System.out.println("mseconds old: " + findAll(oldIndex, entries, 10));
		System.gc();
		System.out.println("mseconds new: " + findAll(oldIndex, entries, 10));
		
		
		/*
		 * Delete elements
		 */
		System.out.println("");
		int numDeleteEntries = (int) (numElements * 0.9);
        System.out.println("Delete " + numDeleteEntries);
        Collections.shuffle(entries, new Random(43));
        List<LLEntry> deleteEntries = entries.subList(0, numDeleteEntries);
        
		System.gc();
		System.out.println("mseconds old: " + PerformanceTest.removeList(oldIndex, deleteEntries));
		oldIndex.write();
		
		System.gc();
		System.out.println("mseconds new: " + PerformanceTest.removeList(newIndex, deleteEntries));
		newIndex.write();
		
		printStats();

	}
	
	/*
	 * iterates through index and returns duration
	 */
	public long iterate(LongLongIndex index, int repetitions) {
        long startTime = System.nanoTime();
		for(int i=0; i<repetitions; i++) {
            LongLongIterator<?> it = index.iterator();
            while(it.hasNext()) {
                    it.next();
            }
		}
		return (System.nanoTime() - startTime) / 1000000;
	}
	
	
	public static long findAll(LongLongIndex index, List<LLEntry> list, int repetitions) {
		long startTime = System.nanoTime();
		for(int i=0; i<repetitions; i++) {
            for (LLEntry entry : list) {
                    index.iterator(entry.getKey(), entry.getKey());
            }
		}
		return (System.nanoTime() - startTime) / 1000000;
	}
	
	
	public void insertAndDeleteMultiple() {
		int numElements = 5000;
        int numDeleteEntries = (int) (numElements * 0.5);
		int numTimes = 10;
		System.out.println("");
		System.out.println("Insert " + numElements + " and delete " + numDeleteEntries +  ", " + numTimes + " times.");

        for(int i=0; i<numTimes; i++) {
            ArrayList<LLEntry> entries = PerformanceTest
                                    .randomEntriesUnique(numElements, 42+i);
            PerformanceTest.insertList(oldIndex, entries);
            oldIndex.write();
            PerformanceTest.insertList(newIndex, entries);
            newIndex.write();

            Collections.shuffle(entries, new Random(43+i));
            List<LLEntry> deleteEntries = entries.subList(0, numDeleteEntries);

            PerformanceTest.removeList(oldIndex, deleteEntries);
            oldIndex.write();
            PerformanceTest.removeList(newIndex, deleteEntries);
            newIndex.write();
        }
        printStats();
		
	}
	
	void printStats() {
		System.out.println("Size "
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetInnerN()) +":" + oldIndex.statsGetLeavesN() + "), "
				+ "(New Index, "
				+ String.valueOf(newIndex.statsGetInnerN()) +":" + newIndex.statsGetLeavesN() + ")"
				);
		System.out.println("Page writes "
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetWrittenPagesN())
				+ "), (New Index, "
				+ String.valueOf(newIndex.getBufferManager()
						.getStatNWrittenPages() + ")"));
		System.out.println("Page reads "
				+ "(Old Index, "
				+ String.valueOf(oldStorage.statsGetReadCount())
				+ "), (New Index, "
				+ String.valueOf(newStorage.statsGetReadCount() + ")"));
	}
}
