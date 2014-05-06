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
package org.zoodb.test.jdo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.Extent;
import javax.jdo.JDOHelper;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.test.api.TestSuper;
import org.zoodb.test.testutil.TestTools;

public class Test_021_MultiSession {
	
	@Before
	public void setUp() {
		TestTools.removeDb();
		TestTools.createDb();
	}
	
	@After
	public void tearDown() {
		TestTools.removeDb();
	}
	
	@Test
	public void testCreateAndCloseSession() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		PersistenceManagerFactory pmf2 = 
			JDOHelper.getPersistenceManagerFactory(props);

		// ************************************************
		// Currently we do not support multiple session.
		// ************************************************
		System.err.println("TODO implement proper in-process multi-session");
		PersistenceManager pm21 = pmf2.getPersistenceManager();
		
		//should have returned different pm's
		assertFalse(pm21 == pm11);

		PersistenceManager pm12 = pmf1.getPersistenceManager();
		//should never return same pm (JDO spec 2.2/11.2)
		assertTrue(pm12 != pm11);

		try {
			pmf1.close();
			fail();
		} catch (JDOUserException e) {
			//good, there are still open session!
		}
		
		assertFalse(pm11.isClosed());
		assertFalse(pm12.isClosed());
		pm11.close();
		pm12.close();
		assertTrue(pm11.isClosed());
		assertTrue(pm12.isClosed());
	
		assertFalse(pm21.isClosed());
		pm21.close();
		assertTrue(pm21.isClosed());

		pmf1.close();
		pmf2.close();
		
		try {
			pmf1.getPersistenceManager();
			fail();
		} catch (JDOUserException e) {
			//good, it's closed!
		}
		
		try {
			pmf1.setConnectionURL("xyz");
			fail();
		} catch (JDOUserException e) {
			//good, there are still open session!
		}
	}
	
	/**
	 * CURRENTLY, only one PMF should be allowed to connect to a database.
	 */
	@Test
	public void testDualSessionAccessFail() {
		ZooJdoProperties props = new ZooJdoProperties(TestTools.getDbName());
		PersistenceManagerFactory pmf1 = 
			JDOHelper.getPersistenceManagerFactory(props);
		PersistenceManager pm11 = pmf1.getPersistenceManager();

		PersistenceManagerFactory pmf2 = 
			JDOHelper.getPersistenceManagerFactory(props);

		try {
			// ************************************************
			// Currently we do not support multiple session.
			// ************************************************
			pmf2.getPersistenceManager();
			fail();
		} catch (JDOUserException e) {
			//good
		}
		
		pm11.close();
		pmf1.close();
	}
	
	
	private abstract static class Worker extends Thread {

		final PersistenceManager pm;
		final int N;
		final int COMMIT_INTERVAL;
		int n = 0;
		final int ID;

		private Worker(int id, int n, int commitInterval) {
			this.ID = id;
			this.pm = ZooJdoHelper.openDB(TestTools.getDbName());
			this.N = n;
			this.COMMIT_INTERVAL = commitInterval;
		}

		@Override
		public void run() {
			try {
				pm.currentTransaction().begin();
				runWorker();
				pm.currentTransaction().rollback();
			} finally {
				closePM(pm);
			}
		}
		
		abstract void runWorker();
	}


	private static class Reader extends Worker {

		private Reader(int id, int n) {
			super(id, n, -1);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void runWorker() {
			Extent<TestSuper> ext = pm.getExtent(TestSuper.class);
			for (TestSuper t: ext) {
				assertTrue(t.getId() >= 0 && t.getId() < N);
				assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
				TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
				assertEquals(t.getId(), t2.getId());
				n++;
			}
			Collection<TestSuper> col = 
					(Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
			for (TestSuper t: col) {
				assertTrue(t.getId() >= 0 && t.getId() < N);
				assertTrue(t.getData()[0] >= 0 && t.getData()[0] < N);
				TestSuper t2 = (TestSuper) pm.getObjectById( JDOHelper.getObjectId(t) );
				assertEquals(t.getId(), t2.getId());
				n++;
			}
		}
	}


	private static class Writer extends Worker {

		private Writer(int id, int n, int commitInterval) {
			super(id, n, commitInterval);
		}

		@Override
		public void runWorker() {
			for (int i = 0; i < N; i++) {
				TestSuper o = new TestSuper(i, i, new long[]{i});
				pm.makePersistent(o);
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					pm.currentTransaction().commit();
					pm.currentTransaction().begin();
				}
			}
		}
	}

	private static class Deleter extends Worker {

		private Deleter(int id, int n, int commitInterval) {
			super(id, n, commitInterval);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void runWorker() {
			Extent<TestSuper> ext = pm.getExtent(TestSuper.class);
			Iterator<TestSuper> iter = ext.iterator();
			while (iter.hasNext() && n < N/2) {
				pm.deletePersistent(iter.next());
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					pm.currentTransaction().commit();
					pm.currentTransaction().begin();
					ext = pm.getExtent(TestSuper.class);
					iter = ext.iterator();
				}
			}
			ext.closeAll();
			
			Collection<TestSuper> col = 
					(Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
			iter = col.iterator();
			while (iter.hasNext() && n < N) {
				pm.deletePersistent(iter.next());
				n++;
				if (n % COMMIT_INTERVAL == 0) {
					pm.currentTransaction().commit();
					pm.currentTransaction().begin();
					col = (Collection<TestSuper>) pm.newQuery(TestSuper.class).execute();
					iter = col.iterator(); 
				}
				if (n == N) {
					break;
				}
			}
		}
	}

	private static void closePM(PersistenceManager pm) {
		if (!pm.isClosed()) {
			if (pm.currentTransaction().isActive()) {
				pm.currentTransaction().rollback();
			}
			pm.close();
		}
	}

	/**
	 * Test concurrent read. 
	 * @throws InterruptedException 
	 */
	@Test
	public void testParallelRead() throws InterruptedException {
		final int N = 10000;
		final int COMMIT_INTERVAL = 1000;
		final int T = 10;

		TestTools.defineSchema(TestSuper.class);
		
		//write
		Writer w = new Writer(0, N, COMMIT_INTERVAL);
		w.start();
		w.join();
		
		//read
		ArrayList<Reader> readers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			readers.add(new Reader(i, N));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals("id=" + reader.ID, 10000 * 2, reader.n);
		}
	}

	/**
	 * Test concurrent write. 
	 * @throws InterruptedException 
	 */
	@Test
	public void testParallelWrite() throws InterruptedException {
		final int N = 10000;
		final int COMMIT_INTERVAL = 1000;
		final int T = 10;

		TestTools.defineSchema(TestSuper.class);
		
		//write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(i, N, COMMIT_INTERVAL));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(10000, w.n);
		}
		
		//read
		Reader r = new Reader(0, N);
		r.start();
		r.join();
		assertEquals(10000 * 2 * T, r.n);
	}

	/**
	 * Test concurrent write. 
	 * @throws InterruptedException 
	 */
	@Test
	public void testParallelReadWrite() throws InterruptedException {
		final int N = 10000;
		final int COMMIT_INTERVAL = 1000;
		final int T = 10;

		TestTools.defineSchema(TestSuper.class);
		
		//read and write
		ArrayList<Thread> workers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			workers.add(new Reader(i, N));
			workers.add(new Writer(i, N, COMMIT_INTERVAL));
		}
		for (Thread w: workers) {
			w.start();
		}
		for (Thread w: workers) {
			w.join();
			if (w instanceof Writer) {
				assertEquals(10000, ((Writer)w).n);
			}
		}
		
		//read only
		ArrayList<Reader> readers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			readers.add(new Reader(i, N));
		}

		for (Reader reader: readers) {
			reader.start();
		}

		for (Reader reader: readers) {
			reader.join();
			assertEquals(10000 * 2 * T, reader.n);
		}
	}

	@Test
	public void testParallelUpdater() throws InterruptedException {
		fail();
	}

	@Test
	public void testParallelDeleter() throws InterruptedException {
		final int N = 10000;
		final int COMMIT_INTERVAL = 1000;
		final int T = 10;

		TestTools.defineSchema(TestSuper.class);
		
		//write
		ArrayList<Writer> writers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			writers.add(new Writer(i, N, COMMIT_INTERVAL));
		}
		for (Writer w: writers) {
			w.start();
		}
		for (Writer w: writers) {
			w.join();
			assertEquals(10000, w.n);
		}
		
		//delete
		ArrayList<Deleter> workers = new ArrayList<>();
		for (int i = 0; i < T; i++) {
			workers.add(new Deleter(i, N, COMMIT_INTERVAL));
		}
		for (Deleter w: workers) {
			w.start();
		}
		for (Deleter w: workers) {
			w.join();
			assertEquals(10000, w.n);
		}
	}
}
