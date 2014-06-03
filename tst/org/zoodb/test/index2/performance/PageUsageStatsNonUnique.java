package org.zoodb.test.index2.performance;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.*;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.test.index2.btree.TestIndex;
import org.zoodb.tools.DBStatistics;
import org.zoodb.tools.ZooConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class PageUsageStatsNonUnique {

    private static final int PAGE_SIZE = 4096;
    private static final boolean MEMORY = true;
    private static final int REPEAT = 10;
    private static final int N_ENTRY = 5000000;

    private static final boolean OLD = false; 
    private static final boolean NEW = true; 
    
    public static void main(String[] args) {
        ZooConfig.setFilePageSize(PAGE_SIZE);
        DBStatistics.enable(true);

        PageUsageStatsNonUnique stats = new PageUsageStatsNonUnique();
        stats.insertAndDelete();
        stats.clear();
        stats.insertAndDeleteMultiple();

        ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
    }

    PagedLongLong oldIndex;
    StorageChannel oldStorage;
    BTreeIndexNonUnique newIndex;
    StorageChannel newStorage;

    public PageUsageStatsNonUnique() {
        this.clear();
    }

    public void clear() {
        if (MEMORY) {
            oldStorage = newMemoryStorage();
        } else {
            oldStorage = TestIndex.newDiskStorage("old_storage.db");
        }
        oldIndex = new PagedLongLong(DATA_TYPE.GENERIC_INDEX, oldStorage);

        if (MEMORY) {
            newStorage = newMemoryStorage();
        } else {
            newStorage = TestIndex.newDiskStorage("new_storage.db");
        }
        newIndex = new BTreeIndexNonUnique(DATA_TYPE.GENERIC_INDEX, newStorage);
    }

    public void insertAndDelete() {
        System.out.println("Orders (inner:leaf), Old: " + (oldIndex.getMaxInnerN() + 1) + ":"
                + (oldIndex.getMaxLeafN() + 1) + "\t" + "New Order: "
                + newIndex.getTree().getPageSize());

        int numElements = N_ENTRY;
        int duplicates = 10;
        ArrayList<LLEntry> entries = PerformanceTest
                .randomEntriesNonUnique(numElements / duplicates, duplicates, 42);
		
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
        if (NEW) System.out.println("mseconds new: " + findAll(oldIndex, entries));
		
		
		/*
		 * Delete elements
		 */
        System.out.println("");
        int numDeleteEntries = (int) (numElements * 0.9);
        System.out.println("Delete " + numDeleteEntries);
        Collections.shuffle(entries, new Random(43));
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

    /*
     * writes and returns the time it took
     */
    public long write(LongLongIndex index) {
        long startTime = System.nanoTime();
        index.write();
        return (System.nanoTime() - startTime) / 1000000;
    }

    /*
     * iterates through index and returns duration
     */
    public long iterate(LongLongIndex index) {
        long startTime = System.nanoTime();
        for(int i=0; i<REPEAT; i++) {
            LongLongIterator<?> it = index.iterator();
            while(it.hasNext()) {
                it.next();
            }
        }
        return (System.nanoTime() - startTime) / 1000000;
    }


    public static long findAll(LongLongIndex index, List<LLEntry> list) {
        long startTime = System.nanoTime();
        for(int i=0; i<REPEAT; i++) {
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
        int numDuplicates = 10;
        System.out.println("");
        System.out.println("Insert " + numElements + " and delete " + numDeleteEntries +  ", " + numTimes + " times.");

        for(int i=0; i<numTimes; i++) {
            ArrayList<LLEntry> entries = PerformanceTest
                    .randomEntriesNonUnique(numElements / numDuplicates, numDuplicates, System.nanoTime()+i);
            if (OLD) PerformanceTest.insertList(oldIndex, entries);
            if (OLD) oldIndex.write();
            if (NEW) PerformanceTest.insertList(newIndex, entries);
            if (NEW) newIndex.write();

            Collections.shuffle(entries, new Random(43+i));
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

    public static StorageChannel newMemoryStorage() {
        return new StorageRootInMemory(
                ZooConfig.getFilePageSize());
    }

}
