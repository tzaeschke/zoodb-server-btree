package org.zoodb.test.index2.performance;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.tools.ZooConfig;

import java.util.ArrayList;

public class PageUsageStats {

	private static final int PAGE_SIZE = 128;

	public static void main(String[] args) {
		ZooConfig.setFilePageSize(PAGE_SIZE);

		PagedUniqueLongLong oldIndex = new PagedUniqueLongLong(
				DATA_TYPE.GENERIC_INDEX, new StorageRootInMemory(
						ZooConfig.getFilePageSize()));

		BTreeIndex newIndex = new BTreeIndex(new StorageRootInMemory(
				ZooConfig.getFilePageSize()), true, true);

		new PageUsageStats(oldIndex, newIndex);

		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}

	public PageUsageStats(PagedUniqueLongLong oldIndex, BTreeIndex newIndex) {
		System.out.println("Old order: " + oldIndex.getMaxInnerN() + ":"
				+ oldIndex.getMaxLeafN() + "\t" + "New Order: "
				+ newIndex.getTree().getLeafOrder());

		int numElements = 1000;
		ArrayList<LLEntry> entries = PerformanceTest
				.increasingEntriesUnique(1000);

		PerformanceTest.insertList(oldIndex, entries);
		oldIndex.write();
		PerformanceTest.insertList(newIndex, entries);
		newIndex.write();

		System.out.println("Size after " + String.valueOf(numElements)
				+ " inserts: " + "(New Index, "
				+ String.valueOf(newIndex.getTree().size()) + ")");

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
