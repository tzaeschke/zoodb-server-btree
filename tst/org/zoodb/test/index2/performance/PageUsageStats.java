package org.zoodb.test.index2.performance;

import java.util.ArrayList;

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
		System.out.println("Old order: " + (oldIndex.getMaxInnerN() + 1) + ":"
				+ (oldIndex.getMaxLeafN() + 1) + "\t" + "New Order: "
				+ newIndex.getTree().getInnerNodeOrder() + ":"
				+ newIndex.getTree().getLeafOrder());

		int numElements = 50000;
		ArrayList<LLEntry> entries = PerformanceTest
				.randomEntriesUnique(numElements);

		System.out.println(PerformanceTest.insertList(oldIndex, entries));
		oldIndex.write();
//		PerformanceTest.removeList(oldIndex, entries);
//		oldIndex.write();
		
		System.out.println(PerformanceTest.insertList(newIndex, entries));
		newIndex.write();
//		PerformanceTest.removeList(newIndex, entries);
//		oldIndex.write();
		
		oldIndex.statsGetInnerN();
		oldIndex.statsGetLeavesN();
		
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

//		System.out.println(newIndex.getTree());
//		oldIndex.print();
		
		System.out.println("Size after " + String.valueOf(numElements)
				+ " inserts: " 
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetInnerN()) +":" + oldIndex.statsGetLeavesN() + "), "
				+ "(New Index, "
				+ String.valueOf(newInnerN) +":" + newLeavesN + ")"

				);

		System.out.println("Page writes after "
				+ String.valueOf(numElements)
				+ " inserts: "
				+ "(Old Index, "
				+ String.valueOf(oldIndex.statsGetWrittenPagesN())
				+ "), (New Index, "
				+ String.valueOf(newIndex.getBufferManager()
						.getStatNWrittenPages() + ")"));
	}
}
