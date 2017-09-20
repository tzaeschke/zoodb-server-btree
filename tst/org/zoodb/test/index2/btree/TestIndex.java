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
package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
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
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.test.index2.performance.PerformanceTest;
import org.zoodb.tools.ZooConfig;

public class TestIndex {

	private IOResourceProvider createPageAccessFile() {
		return new StorageRootInMemory(ZooConfig.getFilePageSize()).createChannel();
	}

	@Test
	public void testWriteRead() {
		IOResourceProvider file = createPageAccessFile();
		BTreeIndexUnique ind1 = new BTreeIndexUnique(PAGE_TYPE.GENERIC_INDEX,
				file);

		ArrayList<LLEntry> entries = PerformanceTest.randomEntriesUnique(1000, new Random(42));
		PerformanceTest.insertList(ind1, entries);
		int rootPageId = file.writeIndex(ind1::write);

		BTreeIndexUnique ind2 = new BTreeIndexUnique(PAGE_TYPE.GENERIC_INDEX,
				file, rootPageId);
		findAll(ind2, entries);
	}
	
	@Test
	public void testWriteReadNonUnique() {
		IOResourceProvider file = createPageAccessFile();
		BTreeIndexNonUnique ind1 = new BTreeIndexNonUnique(PAGE_TYPE.GENERIC_INDEX,
				file);

		ArrayList<LLEntry> entries = 
				PerformanceTest.randomEntriesNonUnique(1000, 10, new Random(42));
		PerformanceTest.insertList(ind1, entries);
		int rootPageId = file.writeIndex(ind1::write);

		BTreeIndexNonUnique ind2 = new BTreeIndexNonUnique(PAGE_TYPE.GENERIC_INDEX,
				file, rootPageId);
		findAll(ind2, entries);
	}
	
	@Test
	public void testWriteReadDifferentValueSize() {
		IOResourceProvider file = createPageAccessFile();
		int keySize = 16;
		int valSize = 4;
		LongLongUIndex ind1 = 
				IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, file, keySize, valSize);

		int numElements = 10000;
		ArrayList<LLEntry> entries = PerformanceTest.randomEntriesUniqueByteValues(numElements,
				42);
		PerformanceTest.insertList(ind1, entries);
		int rootPageId = file.writeIndex(ind1::write);
		LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(
				PAGE_TYPE.GENERIC_INDEX, file, rootPageId, keySize, valSize);
		findAll(ind2, entries);
		
		int numDeleteEntries = (int) (numElements * 0.9);
        Collections.shuffle(entries, new Random(43));
        List<LLEntry> deleteEntries = entries.subList(0, numDeleteEntries);
		
		PerformanceTest.removeList(ind2, deleteEntries);
		rootPageId = file.writeIndex(ind2::write);
//		long startTime = System.nanoTime();
//		System.out.println((System.nanoTime() - startTime) / 1000000);
//		System.out.println(ind1.statsGetWrittenPagesN());

		LongLongUIndex ind3 = IndexFactory.loadUniqueIndex(
				PAGE_TYPE.GENERIC_INDEX, file, rootPageId, keySize, valSize);
		entries.removeAll(deleteEntries);
		findAll(ind3, entries);
	}
	
	@Test
	public void testWriteReadEmptyUnique() {
		/* Unique */
		IOResourceProvider file = createPageAccessFile();
		BTreeIndexUnique ind1 = new BTreeIndexUnique(PAGE_TYPE.GENERIC_INDEX,
				file);

		int rootPageId = file.writeIndex(ind1::write);

		BTreeIndexUnique ind2 = new BTreeIndexUnique(PAGE_TYPE.GENERIC_INDEX,
				file, rootPageId);
		assertEquals(0, ind2.getTree().getRoot().getNumKeys());
	}
	
	@Test
	public void testWriteReadEmptyNonUnique() {
		
		/* Non-Unique */
		IOResourceProvider file = createPageAccessFile();
		BTreeIndexNonUnique ind1 = new BTreeIndexNonUnique(PAGE_TYPE.GENERIC_INDEX,
				file);

		int rootPageId = file.writeIndex(ind1::write);

		BTreeIndexNonUnique ind2 = new BTreeIndexNonUnique(PAGE_TYPE.GENERIC_INDEX,
				file, rootPageId);
		assertEquals(0, ind2.getTree().getRoot().getNumKeys());
	}
	
    @Test
    public void testLoadedPagesNotDirty() {
        final int MAX = 1000000;
        IOResourceProvider paf = createPageAccessFile();
        LongLongUIndex ind = IndexFactory.createUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf);
        for (int i = 1000; i < 1000+MAX; i++) {
            ind.insertLong(i, 32+i);
        }
        int root = paf.writeIndex(ind::write);

        //now read it
        LongLongUIndex ind2 = IndexFactory.loadUniqueIndex(PAGE_TYPE.GENERIC_INDEX, paf, root);
        int w1 = ind2.statsGetWrittenPagesN();
        Iterator<LongLongIndex.LLEntry> i = ind2.iterator(Long.MIN_VALUE, Long.MAX_VALUE);
        int n = 0;
        while (i.hasNext()) {
        	n++;
        	i.next();
        }
        assertEquals(MAX, n);
        paf.writeIndex(ind2::write);
        int w2 = ind2.statsGetWrittenPagesN();
        //no pages written on freshly read root
        assertEquals("w1=" + w1, 0, w1);
        //no pages written when only reading
        assertEquals("w1=" + w1 + "  w2=" + w2, w1, w2);
    }

	public static void findAll(LongLongIndex index, List<LLEntry> list) {
		for (LLEntry entry : list) {
			LongLongIterator<LLEntry> it = index.iterator(entry.getKey(), entry.getKey());
			boolean found = false;
			while(it.hasNext()) {
				if(it.next().getValue() == entry.getValue()) {
					found = true;
				}
			}
			assertTrue(found);
		}
	}
	public static void findAllNonUnique(LongLongIndex index, List<LLEntry> list) {
		for (LLEntry entry : list) {
			LongLongIterator<LLEntry> it = index.iterator(entry.getKey(), entry.getKey());
			assertEquals(it.next().getValue(), entry.getValue());
		}
	}
	
	@Test
	public void testNumPagesDelete() {
		final int MAX = 2000;
		final int MAX_ITER = 50;
		
		IOResourceProvider paf = newMemoryStorage(); 
		
		for (int j = 0; j < MAX_ITER; j++) {
			BTreeIndexNonUnique ind = new BTreeIndexNonUnique(PAGE_TYPE.GENERIC_INDEX,paf);
			BTreeStorageBufferManager bufferManager = ind.getBufferManager();
			
			//First, create objects
			for (int i = 0; i < MAX; i++) {
				ind.insertLong(i, i+32);
			}
			paf.writeIndex(ind::write);
			List<Integer> pageIds = new ArrayList<Integer>();
			BTreeIterator it = new BTreeIterator(ind.getTree());
			while(it.hasNext()) {
				int pageId = ((PagedBTreeNode)it.next()).getPageId();
				pageIds.add(pageId);
			}
			//check that buffer manager does not know more nodes than there are in the tree
			assertEquals(pageIds.size(),bufferManager.getCleanBuffer().size());
			assertEquals(0,bufferManager.getDirtyBuffer().size());
			//now delete them
			for (int i = 0; i < MAX; i++) {
				ind.removeLong(i, i+32);
			}
			//check that buffer manager does not know more nodes than there are in the tree
			assertEquals(0,bufferManager.getCleanBuffer().size());
			assertEquals(1,bufferManager.getDirtyBuffer().size());
			paf.writeIndex(ind::write);
	
			int rootPageId = ind.getTree().getRoot().getPageId();
			pageIds.remove((Object)rootPageId);
			
			// check if all pages are freed
			for(Integer pageId : pageIds) {
				assertTrue(bufferManager.getStorageFile().debugIsPageIdInFreeList(pageId));
			}
			assertFalse(
					bufferManager.getStorageFile().debugIsPageIdInFreeList(rootPageId));
		}
	}
	
	@Test
	public void testNumPagesClear() {
		final int MAX = 2000;
		final int MAX_ITER = 50;
		
		IOResourceProvider paf = newMemoryStorage();
		for (int j = 0; j < MAX_ITER; j++) {
			BTreeIndexNonUnique ind = new BTreeIndexNonUnique(PAGE_TYPE.GENERIC_INDEX,paf);
			//First, create objects
			for (int i = 0; i < MAX; i++) {
				ind.insertLong(i, i+32);
			}
			paf.writeIndex(ind::write);
			
			BTreeStorageBufferManager bufferManager = ind.getBufferManager();
			List<Integer> pageIds = new ArrayList<Integer>();
			BTreeIterator it = new BTreeIterator(ind.getTree());
			while(it.hasNext()) {
				int pageId = ((PagedBTreeNode)it.next()).getPageId();
				pageIds.add(pageId);
			}
			//check that buffer manager does not know more nodes than there are in the tree
			assertEquals(pageIds.size(),bufferManager.getCleanBuffer().size());
			assertEquals(0,bufferManager.getDirtyBuffer().size());
			
			ind.clear();
			
			assertEquals(0,bufferManager.getCleanBuffer().size());
			assertEquals(1,bufferManager.getDirtyBuffer().size());
			
			// check if all pages are freed
			for(Integer pageId : pageIds) {
				assertTrue(bufferManager.getStorageFile().debugIsPageIdInFreeList(pageId));
			}
		}
	}
	
    public static IOResourceProvider newDiskStorage(String filename) {
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
		IOResourceProvider file = new StorageRootFile(filename, "rw",
					ZooConfig.getFilePageSize(), fsm).createChannel();
        fsm.initBackingIndexNew(file);
		
		return file;
	}

    public static IOResourceProvider newMemoryStorage() {
        return new StorageRootInMemory(
                            ZooConfig.getFilePageSize()).createChannel();
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
