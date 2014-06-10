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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.test.index2.btree.TestIndex;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.ZooConfig;

public class PageUsageStats {

	private static final int PAGE_SIZE = 4096;
    private static final boolean MEMORY = true;
    private static final int REPEAT = 10;
    private static final int N_ENTRY = 5000000;

    private static final boolean OLD = true; 
    private static final boolean NEW = true; 
    
    private static final Random R = new Random(0);

    private PagedUniqueLongLong oldIndex;
    private StorageChannel oldStorage; 
    private BTreeIndexUnique newIndex;
    private StorageChannel newStorage;

	public static void main(String[] args) {
		new PageUsageStats().run();
	}

	public void run() {
		ZooConfig.setFilePageSize(PAGE_SIZE);
		DBStatistics.enable(true);

		clear();
		insertAndDelete();
		clear();
		insertAndDeleteMultiple();

		ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
	}

    public void clear() {
    	if (oldStorage != null) {
    		oldStorage.close();
    	}
        if (MEMORY) {
            oldStorage = new StorageRootInMemory(PAGE_SIZE);
        } else {
            oldStorage = TestIndex.newDiskStorage("old_storage.db");
        }
		oldIndex = new PagedUniqueLongLong(DATA_TYPE.GENERIC_INDEX, oldStorage);

    	if (newStorage != null) {
    		newStorage.close();
    	}
		if (MEMORY) {
            newStorage = new StorageRootInMemory(PAGE_SIZE);
        } else {
            newStorage = TestIndex.newDiskStorage("new_storage.db");
        }
		newIndex = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX, newStorage);
	}
			
	public void insertAndDelete() {
		System.out.println("Orders (inner:leaf), Old: " + (oldIndex.getMaxInnerN() + 1) + ":"
				+ (oldIndex.getMaxLeafN() + 1) + "\t" + "New Order: "
				+ newIndex.getTree().getPageSize());

		int numElements = N_ENTRY;
		ArrayList<LLEntry> entries = PerformanceTest.randomEntriesUnique(numElements, R);
		
		/*
		 * Insert elements
		 */
		System.out.println("");
		System.out.println("Insert " + numElements);

		System.gc();
		if (OLD) System.out.println("mseconds old: " + PerformanceTest.insertList(oldIndex, entries));
		System.gc();
		if (NEW) System.out.println("mseconds new: " + PerformanceTest.insertList(newIndex, entries));
		System.out.println("Write");
		if (OLD) System.out.println("mseconds old: " + write(oldIndex));
		if (NEW) System.out.println("mseconds new: " + write(newIndex));
		
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
		if (OLD) System.out.println("mseconds old: " + iterate(oldIndex));
		System.gc();
		if (NEW) System.out.println("mseconds new: " + iterate(newIndex));
		
        /*
		 * Find all
		 */
        System.out.println("");
		System.out.println("Find all " + numElements);
		System.gc();
		if (OLD) System.out.println("mseconds old: " + findAll(oldIndex, entries));
		System.gc();
		if (NEW) System.out.println("mseconds new: " + findAll(newIndex, entries));
		
		
		/*
		 * Delete elements
		 */
		System.out.println("");
		int numDeleteEntries = (int) (numElements * 0.9);
        System.out.println("Delete " + numDeleteEntries);
        Collections.shuffle(entries, R);
        List<LLEntry> deleteEntries = entries.subList(0, numDeleteEntries);
        
		System.gc();
		if (OLD) System.out.println("mseconds old: " + PerformanceTest.removeList(oldIndex, deleteEntries));
		System.gc();
		if (NEW) System.out.println("mseconds new: " + PerformanceTest.removeList(newIndex, deleteEntries));
		System.out.println("Write");
		if (OLD) System.out.println("mseconds old: " + write(oldIndex));
		if (NEW) System.out.println("mseconds new: " + write(newIndex));
		
		printStats();
	}
	
	/**
	 * writes and returns the time it took
	 */
	public long write(LongLongIndex index) {
        long startTime = System.nanoTime();
        index.write();
		return (System.nanoTime() - startTime) / 1000000;
	}
	
	/**
	 * iterates through index and returns duration
	 */
	public long iterate(LongLongIndex index) {
        long startTime = System.nanoTime();
		for (int i=0; i<REPEAT; i++) {
            LongLongIterator<?> it = index.iterator();
            while (it.hasNext()) {
            	it.next();
            }
		}
		return (System.nanoTime() - startTime) / 1000000;
	}
	
	
	public static long findAll(LongLongIndex index, List<LLEntry> list) {
		long startTime = System.nanoTime();
		for (int i=0; i<REPEAT; i++) {
            for (LLEntry entry : list) {
            	index.iterator(entry.getKey(), entry.getKey());
            }
		}
		return (System.nanoTime() - startTime) / 1000000;
	}
	
	
	public void insertAndDeleteMultiple() {
		int numElements = 5000;
        int numDeleteEntries = (int) (numElements * 0.5);
		int numTimes = REPEAT;
		System.out.println("");
		System.out.println("Insert " + numElements + " and delete " + numDeleteEntries +  ", " + numTimes + " times.");

        for(int i=0; i<numTimes; i++) {
            ArrayList<LLEntry> entries = PerformanceTest.randomEntriesUnique(numElements, R);
            if (OLD) PerformanceTest.insertList(oldIndex, entries);
            if (OLD) oldIndex.write();
            if (NEW) PerformanceTest.insertList(newIndex, entries);
            if (NEW) newIndex.write();

            Collections.shuffle(entries, R);
            List<LLEntry> deleteEntries = entries.subList(0, numDeleteEntries);

            if (OLD) PerformanceTest.removeList(oldIndex, deleteEntries);
            if (OLD) oldIndex.write();
            if (NEW) PerformanceTest.removeList(newIndex, deleteEntries);
            if (NEW) newIndex.write();
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
				+ String.valueOf(oldIndex.getStorageChannel().statsGetWriteCount())
				+ "), (New Index, "
				+ String.valueOf(newIndex.getBufferManager().getStorageFile().statsGetWriteCount()
						+ ")"));
		System.out.println("Page reads "
				+ "(Old Index, "
				+ String.valueOf(oldStorage.statsGetReadCount())
				+ "), (New Index, "
				+ String.valueOf(newStorage.statsGetReadCount() + ")"));
	}
}
