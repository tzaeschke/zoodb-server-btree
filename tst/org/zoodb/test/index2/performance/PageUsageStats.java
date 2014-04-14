package org.zoodb.test.index2.performance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.tools.ZooConfig;

public class PageUsageStats {

	private static final int PAGE_SIZE = 128;

	public static void main(String[] args) {
		ZooConfig.setFilePageSize(PAGE_SIZE);

		StorageChannel oldStorage = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		PagedUniqueLongLong oldIndex = new PagedUniqueLongLong(
				DATA_TYPE.GENERIC_INDEX, oldStorage);

		StorageChannel newStorage = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		BTreeIndex newIndex = new BTreeIndex(newStorage, true, true);

		new PageUsageStats(oldIndex, oldStorage, newIndex, newStorage);

		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}

	public PageUsageStats(PagedUniqueLongLong oldIndex,
			StorageChannel oldStorage, BTreeIndex newIndex,
			StorageChannel newStorage) {
		System.out.println("Orders (inner:leaf), Old: " + (oldIndex.getMaxInnerN() + 1) + ":"
				+ (oldIndex.getMaxLeafN() + 1) + "\t" + "New Order: "
				+ newIndex.getTree().getInnerNodeOrder() + ":"
				+ newIndex.getTree().getLeafOrder());

		int numElements = 10;
		ArrayList<LLEntry> entries = PerformanceTest
				.randomEntriesUnique(numElements, 42);
		
		/*
		 * Insert elements
		 */
		System.out.println("");
		System.out.println("Insert " + numElements);

		System.out.println("mseconds old: " + PerformanceTest.insertList(oldIndex, entries));
		oldIndex.write();
		
		System.out.println("mseconds new: " + PerformanceTest.insertList(newIndex, entries));
		newIndex.write();
		
		BTreeIterator it = new BTreeIterator(newIndex.getTree());
		int height = 1;
		while(it.hasNext()) {
			if(it.next().isLeaf()) break;
			height++;
		}
		System.out.println("Height new Index: " + height);
		
		it = new BTreeIterator(newIndex.getTree());
		int newInnerN = 0;
		int newLeavesN = 0;
		while(it.hasNext()) {
			if(it.next().isLeaf()) newLeavesN++;
			else newInnerN++;
		}

		System.out.println("Size "
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetInnerN()) +":" + oldIndex.statsGetLeavesN() + "), "
				+ "(New Index, "
				+ String.valueOf(newInnerN) +":" + newLeavesN + ")"

				);

		System.out.println("Page writes "
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetWrittenPagesN())
				+ "), (New Index, "
				+ String.valueOf(newIndex.getBufferManager()
						.getStatNWrittenPages() + ")"));
		
		System.out.println(newIndex.getTree());
		
		/*
		 * Delete elements
		 */
		System.out.println("");
		int numDeleteEntries = 5; 
        System.out.println("Delete " + numDeleteEntries);
        Collections.shuffle(entries, new Random(43));
        List<LLEntry> deleteEntries = entries.subList(0, numDeleteEntries);

		System.out.println("mseconds old: " + PerformanceTest.removeList(oldIndex, deleteEntries));
		oldIndex.write();
		
		System.out.println("mseconds new: " + PerformanceTest.removeList(newIndex, deleteEntries));
		newIndex.write();
		
		it = new BTreeIterator(newIndex.getTree());
		newInnerN = 0;
		newLeavesN = 0;
		while(it.hasNext()) {
			if(it.next().isLeaf()) newLeavesN++;
			else newInnerN++;
		}
		
        int deletedIndex = 0;
		for(LLEntry entry : deleteEntries) {
			LLEntry e = newIndex.findValue(entry.getKey());
			if(-1 != e.getValue()) {
				throw new RuntimeException("Can find " + deletedIndex + "th element despite deletion.");
			}
			deletedIndex++;
		}

		System.out.println("Size "
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetInnerN()) +":" + oldIndex.statsGetLeavesN() + "), "
				+ "(New Index, "
				+ String.valueOf(newInnerN) +":" + newLeavesN + ")"

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
