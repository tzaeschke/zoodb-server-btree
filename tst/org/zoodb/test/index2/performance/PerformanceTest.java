/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 *
 * This file is part of ZooDB.
 *
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 *
 * See the README and COPYING files for further information.
 */
package org.zoodb.test.index2.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.index.AbstractPagedIndex;
import org.zoodb.internal.server.index.BTreeIndex;
import org.zoodb.internal.server.index.BTreeIndexNonUnique;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.internal.server.index.PagedLongLong;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.test.index2.btree.TestIndex;
import org.zoodb.tools.ZooConfig;

public class PerformanceTest {

	private static final boolean USE_CONSOLE = false;
	
	private static final int PAGE_SIZE = 4096;
	private final int numExperiments = 11;
	// number of repetitions of a particular operation
	private static String fileName = "./tst/org/zoodb/test/index2/performance/performanceTest.csv";
	private final OutputStreamWriter STDOUT = new OutputStreamWriter(System.out);
	private final DATA_TYPE dataType = DATA_TYPE.GENERIC_INDEX;
	private BufferedWriter fileWriter;

	public static void main(String[] args) {
		ZooConfig.setFilePageSize(PAGE_SIZE);
		File fileTemp = new File(fileName);
		if (fileTemp.exists()) {
			fileTemp.delete();
		}
		new PerformanceTest();

		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}

	public PerformanceTest() {
		List<LongLongIndex> indices = new ArrayList<LongLongIndex>();
		indices.add(new PagedUniqueLongLong(dataType, TestIndex
				.newDiskStorage("perfTestOldUnique")));
		indices.add(new PagedLongLong(dataType, TestIndex.newDiskStorage("perfTestOldNonUnique")));
		indices.add(new BTreeIndexUnique(dataType, TestIndex.newDiskStorage("perfTestNewUnique.zdb")));
		indices.add(new BTreeIndexNonUnique(dataType, TestIndex
				.newDiskStorage("perfTestNewNonUnique")));

		try {
			if (USE_CONSOLE) {
				this.fileWriter = new BufferedWriter(STDOUT);
			} else {
				this.fileWriter = new BufferedWriter(new FileWriter(fileName));
			}
			this.printHeader();
			for (LongLongIndex index : indices) {
				System.out.println("Index: " + stringType(index) + " "
						+ stringUnique(index));
				System.out.println("\tinsert");
				index.clear();
				this.insertPerformance(index);
				System.out.println("\tsearch");
				index.clear();
				this.searchPerformance(index);
				System.out.println("\tremove");
				index.clear();
				this.removePerformance(index);
			}
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void printHeader() throws IOException {
		fileWriter
				.write("IndexType,IndexUnique,Operation,ListType,numElements,ExperimentNumber,Duration,NumNodes");
		fileWriter.newLine();
	}

	private void printLine(LongLongIndex index, String operation,
			String listType, int numElements, int experimentNumber,
			long duration) {
		try {
			fileWriter.write(stringType(index)
					+ ","
					+ stringUnique(index)
					+ ","
					+ operation
					+ ","
					+ listType
					+ ","
					+ Integer.toString(numElements)
					+ ","
					+ Integer.toString(experimentNumber)
					+ ","
					+ Long.toString(duration)
					+ ","
					+ Long.toString(index.statsGetInnerN()
							+ index.statsGetLeavesN()));
			fileWriter.newLine();
		} catch (Exception e) {
		}
	}

	private void insertPerformance(LongLongIndex index) {
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(

		Arrays.asList(100000, 500000, 1000000));
		for (int numElements : numElementsArray) {
			insertPerformanceHelper(index, randomEntriesUnique(numElements), "random");
		}

		numElementsArray = new ArrayList<Integer>(Arrays.asList(100000, 500000,
				1000000));
		for (int numElements : numElementsArray) {
			insertPerformanceHelper(index,
					increasingEntriesUnique(numElements), "increasing");
		}

		if (!isUnique(index)) {
			numElementsArray = new ArrayList<Integer>(Arrays.asList(100000, 500000,
					1000000));
			for (int numElements : numElementsArray) {
				int numDuplicates = 10;
				insertPerformanceHelper(
						index,
						randomEntriesNonUnique(numElements / numDuplicates,
								numDuplicates), "random_nonUnique");
			}
		}
	}

	private void insertPerformanceHelper(LongLongIndex index,
			ArrayList<LLEntry> entries, String entriesName) {
		for (int i = 0; i < this.numExperiments; i++) {
			index.clear();
			long duration = insertList(index, entries);
			printLine(index, "insert", entriesName, entries.size(), i, duration);
			printLine(index, "insert_write", entriesName, entries.size(), i,
					timeWrite(index));
		}
	}

	private void searchPerformance(LongLongIndex index) {
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(
				Arrays.asList(100000, 500000, 1000000));

		for (int numElements : numElementsArray) {
			if (isUnique(index)) {
				searchPerformanceHelper(index,
						randomEntriesUnique(numElements), "random");
			} else {
				int numDuplicates = 10;
				searchPerformanceHelper(
						index,
						randomEntriesNonUnique(numElements / numDuplicates,
								numDuplicates), "random_nonUnique");
			}
		}

	}

	private void searchPerformanceHelper(LongLongIndex index,
			ArrayList<LLEntry> entryList, String entryListName) {
		for (int i = 0; i < this.numExperiments; i++) {
			index.clear();
			insertList(index, entryList);
			long duration = searchList(index, entryList);
			printLine(index, "search", entryListName, entryList.size(), i,
					duration);
		}
	}

	private void removePerformance(LongLongIndex index) {
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(
				Arrays.asList(200000, 500000));
		for (int numElements : numElementsArray) {
			if (isUnique(index)) {
				removePerformanceHelper(index,
						randomEntriesUnique(numElements), "random");
			} else {
				int numDuplicates = 10;
				removePerformanceHelper(
						index,
						randomEntriesNonUnique(numElements / numDuplicates,
								numDuplicates), "random_nonUnique");
			}
		}
	}

	private void removePerformanceHelper(LongLongIndex index,
			ArrayList<LLEntry> entryList, String entryListName) {
		for (int i = 0; i < this.numExperiments; i++) {
			index.clear();
			insertList(index, entryList);
			long duration = removeList(index, entryList);
			printLine(index, "remove", entryListName, entryList.size(), i,
					duration);
		}
	}

	public static long insertList(LongLongIndex index, List<LLEntry> list) {
		long startTime = System.nanoTime();
		for (LLEntry entry : list) {
			index.insertLong(entry.getKey(), entry.getValue());
		}
		return (System.nanoTime() - startTime) / 1000000;
	}

	/*
	 * Searches every element from the list in the index and returns its
	 * duration.
	 */
	public static long searchList(LongLongIndex index, List<LLEntry> list) {
		long startTime = System.nanoTime();
		for (LLEntry entry : list) {
			LLEntryIterator it = index.iterator(entry.getKey(), entry.getKey());
			while (it.hasNext()) {
				it.next();
			}
		}
		return (System.nanoTime() - startTime) / 1000000;
	}

	/*
	 * Removes every element from the list in the index and returns its duration
	 */
	public static long removeList(LongLongIndex index, List<LLEntry> list) {
		long startTime = System.nanoTime();
		for (LLEntry entry : list) {
			index.removeLong(entry.getKey(), entry.getValue());
		}
		return (System.nanoTime() - startTime) / 1000000;
	}

	public static ArrayList<LLEntry> randomEntriesUnique(int numElements) {
		return randomEntriesUnique(numElements, new Random(0));
	}

	public static ArrayList<LLEntry> randomEntriesUnique(int numElements, Random R) {
		// ensure that entries with equal keys can not exists in the set
		Set<LLEntry> randomEntryList = new TreeSet<LLEntry>(
				new Comparator<LLEntry>() {
					public int compare(LLEntry e1, LLEntry e2) {
						return Long.compare(e1.getKey(), e2.getKey());
					}
				});
		while (randomEntryList.size() < numElements) {
			randomEntryList.add(new LLEntry(R.nextLong(), R.nextLong()));
		}
		ArrayList<LLEntry> l = new ArrayList<LLEntry>(randomEntryList);
		Collections.shuffle(l, R);
		return l;
	}

	public static ArrayList<LLEntry> randomEntriesNonUnique(int numElements,
			int numKeyDuplicates) {
		return randomEntriesNonUnique(numElements, numKeyDuplicates, new Random(0));
	}

	public static ArrayList<LLEntry> randomEntriesNonUnique(int numElements,
			int numKeyDuplicates, Random R) {
		ArrayList<LLEntry> entries = new ArrayList<LLEntry>();
		
		for (int i = 0; i < numKeyDuplicates; i++) {
			long key = R.nextLong();
			for (int j = 0; j < numElements; j++) {
				entries.add(new LLEntry(key, R.nextLong()));
			}
		}
		return entries;
	}

	public static ArrayList<LLEntry> randomEntriesUniqueByteValues(
			int numElements, long seed) {
		// ensure that entries with equal keys can not exists in the set
		Set<LLEntry> randomEntryList = new TreeSet<LLEntry>(
				new Comparator<LLEntry>() {
					public int compare(LLEntry e1, LLEntry e2) {
						return Long.compare(e1.getKey(), e2.getKey());
					}
				});
		Random prng = new Random(seed);
		while (randomEntryList.size() < numElements) {
			byte[] byteList = new byte[1];
			prng.nextBytes(byteList);
			randomEntryList.add(new LLEntry(prng.nextInt(), byteList[0]));
		}
		ArrayList<LLEntry> l = new ArrayList<LLEntry>(randomEntryList);
		Collections.shuffle(l, prng);
		return l;
	}

	/*
	 * generates a list of increasing integers beginning with a random element
	 */
	public static ArrayList<LLEntry> increasingEntriesUnique(int numElements) {
		// ensure that entries with equal keys can not exists in the set
		Random prng = new Random(System.nanoTime());
		long startElement = Math.abs(prng.nextLong());
		ArrayList<LLEntry> entryList = new ArrayList<LLEntry>();
		for (long i = 0; i < numElements; i++) {
			entryList.add(new LLEntry(i + startElement, i + startElement));
		}
		return entryList;
	}

	public static ArrayList<LLEntry> decreasingEntriesUnique(int numElements) {
		// ensure that entries with equal keys can not exists in the set
		long startElement = 0;
		ArrayList<LLEntry> entryList = new ArrayList<LLEntry>();
		for (long i = 0; i < numElements; i++) {
			entryList.add(new LLEntry(numElements - i + startElement,
					numElements - i + startElement));
		}
		return entryList;
	}

	public boolean isUnique(LongLongIndex index) {
		if (index instanceof BTreeIndexUnique
				|| index instanceof PagedUniqueLongLong) {
			return true;
		} else if (index instanceof BTreeIndexNonUnique
				|| index instanceof PagedLongLong) {
			return false;
		} else {
			throw new RuntimeException();
		}

	}

	public String stringUnique(LongLongIndex index) {
		return isUnique(index) ? "Unique" : "nonUnique";
	}

	public String stringType(LongLongIndex index) {
		if (index instanceof AbstractPagedIndex) {
			return "old";
		} else if (index instanceof BTreeIndex) {
			return "new";
		} else {
			throw new RuntimeException();
		}
	}

	public long timeWrite(LongLongIndex index) {
		long startTime = System.nanoTime();
		index.write();
		return (System.nanoTime() - startTime) / 1000000;
	}
}
