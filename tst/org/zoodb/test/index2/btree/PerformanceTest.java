package org.zoodb.test.index2.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.tools.ZooConfig;

public class PerformanceTest {

	private static final int PAGE_SIZE = 128;

	public static void main(String[] args) {
		ZooConfig.setFilePageSize(PAGE_SIZE);
		PagedUniqueLongLong index = new PagedUniqueLongLong(
				DATA_TYPE.GENERIC_INDEX, new StorageRootInMemory(
						ZooConfig.getFilePageSize()));
		new PerformanceTest(index);
		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}

	public PerformanceTest(LongLongUIndex index) {
		index.clear();
		this.insertPerformance(index);
		index.clear();
		this.removePerformance(index);
		index.clear();
		this.searchPerformance(index);

	}

	private void insertPerformance(LongLongUIndex index) {
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(
				Arrays.asList(10, 20, 50, 100, 200, 500, 1000, 2000, 5000,
						10000, 20000, 50000, 100000, 200000, 1000000));

		for (int numElements : numElementsArray) {
			ArrayList<LLEntry> entries = randomEntriesUnique(numElements);
			long startTime = System.nanoTime();
			for (LLEntry entry : entries) {
				index.insertLong(entry.getKey(), entry.getValue());
			}
			long executionTime = (System.nanoTime() - startTime) / 1000000;
			System.out.println(executionTime);
		}

	}

	private ArrayList<LLEntry> randomEntriesUnique(int numElements) {
		// ensure that entries with equal keys can not exists in the set
		Set<LLEntry> randomEntryList = new TreeSet<LLEntry>(
				new Comparator<LLEntry>() {
					public int compare(LLEntry e1, LLEntry e2) {
						return Long.compare(e1.getKey(), e2.getKey());
					}
				});
		Random prng = new Random(System.nanoTime());
		while (randomEntryList.size() < numElements) {
			randomEntryList.add(new LLEntry(prng.nextLong(), prng.nextLong()));
		}
		return new ArrayList<LLEntry>(randomEntryList);
	}

	private void removePerformance(LongLongUIndex index) {
		// TODO Auto-generated method stub

	}

	private void searchPerformance(LongLongUIndex index) {
		// TODO Auto-generated method stub

	}

}
