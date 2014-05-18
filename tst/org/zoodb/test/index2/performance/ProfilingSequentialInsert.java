package org.zoodb.test.index2.performance;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.PagedUniqueLongLong;
import org.zoodb.tools.ZooConfig;

public class ProfilingSequentialInsert {

    private static final int PAGE_SIZE = 256;
    private static BTreeIndexUnique newIndex;
    private static StorageChannel newStorage;

    private static PagedUniqueLongLong oldIndex;
    private static StorageChannel oldStorage;

    public static void main(String[] args) {
        int numEntries = 10000000;
        ZooConfig.setFilePageSize(PAGE_SIZE);
        initStorage();
        insertSequential(numEntries);
        ZooConfig.setFilePageSize(ZooConfig.FILE_PAGE_SIZE_DEFAULT);
    }

    private static void insertSequential(int numEntries) {
        for (int i = 0; i < numEntries; i++) {
            //oldIndex.insertLong(i, i);
            newIndex.insertLong(i, i);
        }
    }

    private static void initStorage() {
        newStorage = newMemoryStorage();
        newIndex = new BTreeIndexUnique(DiskIO.DATA_TYPE.GENERIC_INDEX, newStorage);

        oldStorage = newMemoryStorage();
        oldIndex = new PagedUniqueLongLong(
                DiskIO.DATA_TYPE.GENERIC_INDEX, oldStorage);
    }

    public static StorageChannel newMemoryStorage() {
        return new StorageRootInMemory(
                ZooConfig.getFilePageSize());
    }
}
