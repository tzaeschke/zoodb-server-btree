package org.zoodb.test.index2.performance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
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
	private final int numExperiments = 20;
	// number of repetitions of a particular operation
	private final String fileName = "./tst/org/zoodb/test/index2/performance/performanceTest.csv";
	private final OutputStreamWriter STDOUT = new OutputStreamWriter(System.out);
	private BufferedWriter fileWriter;

	public static void main(String[] args) {
		ZooConfig.setFilePageSize(PAGE_SIZE);
		
		PagedUniqueLongLong index = new PagedUniqueLongLong(
				DATA_TYPE.GENERIC_INDEX, new StorageRootInMemory(
						ZooConfig.getFilePageSize()));
		new PerformanceTest(index);
		
		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}

	public PerformanceTest(LongLongUIndex index) {
		try {
			this.fileWriter = new BufferedWriter(new FileWriter(fileName));
//			this.fileWriter = new BufferedWriter(STDOUT);
            this.printHeader();
            index.clear();
            this.insertPerformance(index);
            index.clear();
            this.searchPerformance(index);
            index.clear();
            this.removePerformance(index);
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void printHeader() throws IOException {
		fileWriter.write("Operation,ListType,numElements,ExperimentNumber,Duration");
        fileWriter.newLine();
	}

	private void printLine(String operation, String listType, int numElements,
			int experimentNumber, long duration) throws IOException{
		fileWriter.write(operation + "," + listType + ","
				+ Integer.toString(numElements) + ","
				+ Integer.toString(experimentNumber) + ","
				+ Long.toString(duration));
        fileWriter.newLine();
	}

	private void insertPerformance(LongLongUIndex index) throws IOException{
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(
				Arrays.asList(200000, 500000, 1000000));

		for (int numElements : numElementsArray) {
			for (int i = 0; i < this.numExperiments; i++) {
				index.clear();
				long duration = insertList(index,
						randomEntriesUnique(numElements));
				printLine("insert", "random", numElements, i, duration);
			}
		}
		
		numElementsArray = new ArrayList<Integer>(
				Arrays.asList(500000, 1000000, 2000000));
        for (int numElements : numElementsArray){
			for (int i = 0; i < this.numExperiments; i++) {
				index.clear();
				long duration = insertList(index,
						increasingEntriesUnique(numElements));
				printLine("insert", "increasing", numElements, i, duration);
			}
		}
	}

	private void searchPerformance(LongLongUIndex index) throws IOException {
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(
				Arrays.asList(200000, 500000, 1000000));

		for (int numElements : numElementsArray) {
			for (int i = 0; i < this.numExperiments; i++) {
				index.clear();
				List<LLEntry> entryList = randomEntriesUnique(numElements);
				insertList(index, entryList);
				long duration = searchList(index, entryList);
				printLine("search", "random", numElements, i, duration);
			}
		}
	}

	private void removePerformance(LongLongUIndex index) throws IOException {
		ArrayList<Integer> numElementsArray = new ArrayList<Integer>(
				Arrays.asList(200000, 500000, 1000000));

		for (int numElements : numElementsArray) {
			for (int i = 0; i < this.numExperiments; i++) {
				index.clear();
				List<LLEntry> entryList = randomEntriesUnique(numElements);
				insertList(index, entryList);
				long duration = removeList(index, entryList);
				printLine("remove", "random", numElements, i, duration);
			}
		}
	}

	private long insertList(LongLongUIndex index, List<LLEntry> list) {
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
	private long searchList(LongLongUIndex index, List<LLEntry> list) {
		long startTime = System.nanoTime();
		for (LLEntry entry : list) {
			index.findValue(entry.getKey());
		}
		return (System.nanoTime() - startTime) / 1000000;
	}

	/*
	 * Removes every element from the list in the index and returns its duration
	 */
	private long removeList(LongLongUIndex index, List<LLEntry> list) {
		long startTime = System.nanoTime();
		for (LLEntry entry : list) {
			index.removeLong(entry.getKey());
		}
		return (System.nanoTime() - startTime) / 1000000;
	}

	private static ArrayList<LLEntry> randomEntriesUnique(int numElements) {
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

	/*
	 * generates a list of increasing integers beginning with a random element
	 */
	private static ArrayList<LLEntry> increasingEntriesUnique(int numElements) {
		// ensure that entries with equal keys can not exists in the set
		Random prng = new Random(System.nanoTime());
		int startElement = Math.abs(prng.nextInt());
		ArrayList<LLEntry> entryList = new ArrayList<LLEntry>();
		for (long i = 0; i < numElements; i++) {
			entryList.add(new LLEntry(i + startElement, i + startElement));
		}
		return entryList;
	}

	private static ArrayList<LLEntry> decreasingEntriesUnique(int numElements) {
		// ensure that entries with equal keys can not exists in the set
		Random prng = new Random(System.nanoTime());
		long startElement = 0;
		ArrayList<LLEntry> entryList = new ArrayList<LLEntry>();
		for (long i = 0; i < numElements; i++) {
			entryList.add(new LLEntry(numElements - i + startElement,
					numElements - i + startElement));
		}
		return entryList;
	}

}
