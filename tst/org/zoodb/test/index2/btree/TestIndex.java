package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootFile;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.BTreeIndexNonUnique;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.FreeSpaceManager;
import org.zoodb.internal.server.index.IndexFactory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LongLongIterator;
import org.zoodb.internal.server.index.LongLongIndex.LongLongUIndex;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.test.index2.performance.PerformanceTest;
import org.zoodb.tools.ZooConfig;

public class TestIndex {

	private StorageChannel createPageAccessFile() {
		StorageChannel paf = new StorageRootInMemory(
				ZooConfig.getFilePageSize());
		return paf;
	}

	@Test
	public void testWriteRead() {
		StorageChannel file = createPageAccessFile();
		BTreeIndexUnique ind1 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file);

		ArrayList<LLEntry> entries = PerformanceTest.randomEntriesUnique(1000,
				42);
		PerformanceTest.insertList(ind1, entries);
		int rootPageId = ind1.write();

		BTreeIndexUnique ind2 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file, rootPageId);
		findAll(ind2, entries);
	}
	
	@Test
	public void testWriteReadEmptyUnique() {
		/* Unique */
		StorageChannel file = createPageAccessFile();
		BTreeIndexUnique ind1 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file);

		int rootPageId = ind1.write();

		BTreeIndexUnique ind2 = new BTreeIndexUnique(DATA_TYPE.GENERIC_INDEX,
				file, rootPageId);
		assertEquals(0, ind2.getTree().getRoot().getNumKeys());
	}
	
	@Test
	public void testWriteReadEmptyNonUnique() {
		
		/* Non-Unique */
		StorageChannel file = createPageAccessFile();
		BTreeIndexNonUnique ind1 = new BTreeIndexNonUnique(DATA_TYPE.GENERIC_INDEX,
				file);

		int rootPageId = ind1.write();

		BTreeIndexNonUnique ind2 = new BTreeIndexNonUnique(DATA_TYPE.GENERIC_INDEX,
				file, rootPageId);
		assertEquals(0, ind2.getTree().getRoot().getNumKeys());
	}
	
    @Test
    public void testLoadedPagesNotDirty() {
        final int MAX = 1000000;
        StorageChannel paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = ind.write();

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(DATA_TYPE.GENERIC_INDEX, paf, root);
        int w1 = ind2.statsGetWrittenPagesN();
        Iterator<LongLongIndex.LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        int n = 0;
        while (i.hasNext()) {
        	n++;
        	i.next();
        }
        ind2.write();
        int w2 = ind2.statsGetWrittenPagesN();
        //no pages written on freshly read root
        assertEquals("w1=" + w1, 0, w1);
        //no pages written when only reading
        assertEquals("w1=" + w1 + "  w2=" + w2, w1, w2);
    }

	public static void findAll(LongLongIndex index, List<LLEntry> list) {
		for (LLEntry entry : list) {
			LongLongIterator<LLEntry> it = index.iterator(entry.getKey(), entry.getKey());
			assertEquals(it.next().getValue(), entry.getValue());
		}
	}
	
	/* 
	 * !!!!!!! TEST FAILS !!!!!!!!!!!!
	 * should write the FreeSpaceManager for the reported free pages to have effect
	 * BUT StorageRootFile does not provide the FreeSpaceManager
	 */
	@Test
	public void testObjectsReusePagesDroppedMulti() {
		final int MAX = 2000;
		final int MAX_ITER = 50;
		
		String dbName = "TestIndex.zdb";
		StorageChannel paf = newDiskStorage(dbName); 
		File f = new File(toPath(dbName));
		System.out.println(f.length());

		long len1 = -1;
		
		for (int j = 0; j < MAX_ITER; j++) {
			BTreeIndexNonUnique ind = new BTreeIndexNonUnique(DATA_TYPE.GENERIC_INDEX,paf);
			
			//First, create objects
			for (int i = 0; i < MAX; i++) {
				ind.insertLong(i, i+32);
			}
			ind.write();
			//now delete them
			for (int i = 0; i < MAX; i++) {
				ind.removeLong(i, i+32);
			}
			ind.write();
	
			//check length only from 3rd iteration on...
			if (j == 3) {
				len1 = f.length();
			}
		}

		//check that the new Objects reused previous pages
		int ps = ZooConfig.getFilePageSize();
		assertTrue("l1=" + len1/ps + " l2=" + f.length()/ps, 
				(len1*1.1 > f.length()) || (f.length()/ps - len1/ps < 20));
	}
	
    public static StorageChannel newDiskStorage(String filename) {
	    String dbPath = toPath(filename);
        String folderPath = dbPath.substring(0, dbPath.lastIndexOf(File.separator));
        File dbDir = new File(folderPath);
        if (!dbDir.exists()) {
            createDbFolder(dbDir);
        }

		filename = toPath(filename);
		File dbFile = new File(filename);
		if (dbFile.exists()) {
			dbFile.delete();
		}
		try {
			dbFile.createNewFile();
		} catch (Exception e) {
			throw DBLogger.newUser(dbFile.getPath() + " "+ e.toString());
		}
		
		FreeSpaceManager fsm = new FreeSpaceManager();
		StorageChannel file = new StorageRootFile(filename, "rw",
					ZooConfig.getFilePageSize(), fsm);
        fsm.initBackingIndexNew(file);
		
		return file;
	}

    public static StorageChannel newMemoryStorage() {
        return new StorageRootInMemory(
                            ZooConfig.getFilePageSize());
    }

    public static void createDbFolder(File dbDir) {
		if (dbDir.exists()) {
		    return;
			//throw new JDOUserException("ZOO: Repository exists: " + dbFolder);
		}
		boolean r = dbDir.mkdirs();
		if (!r) {
			throw DBLogger.newUser("Could not create folders: " + dbDir.getAbsolutePath());
		}
	}
	
	public static String toPath(String dbName) {
	    if (dbName.contains("\\") || dbName.contains("/") || dbName.contains(File.separator)) {
	        return dbName;
	    }
	    String DEFAULT_FOLDER = System.getProperty("user.home") + File.separator + "zoodb"; 
	    return DEFAULT_FOLDER + File.separator + dbName;
	}
	

}
