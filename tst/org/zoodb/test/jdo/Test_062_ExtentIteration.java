/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

public class Test_062_ExtentIteration {

	@BeforeClass
	public static void setUp() {
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}
	
	@Before
	public void beforeTest() {
        TestTools.createDb();
        TestTools.defineSchema(TestClass.class, TestClassTiny.class, TestClassTiny2.class);
	}

	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
    /**
     * Tests extent across transaction boundaries.
     * The problem here is that extents are pos-indices, which are COWed.
     * So during a commit, object may get rewritten, changing the index,
     * meaning that the positions in the extents are wrong.
     * 
     * In this test, only the values in the loaded objects are false. But the
     * pages they are loaded from are actually free, so they may get overwritten
     * by other data.
     */
    @Test
    public void testExtentWithModificationAhead() {
        int N = 10000;
        PersistenceManager pm = TestTools.openPM();
        //pm.setIgnoreCache(false);
        pm.currentTransaction().begin();

        for (int i = 0; i < N; i++) {
            TestClass tc = new TestClass();
            tc.setInt(i);
            pm.makePersistent(tc);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //modify all --> create empty space in the beginning of the DB
        for (TestClass tc: pm.getExtent(TestClass.class)) {
            tc.setLong(12);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //start iterating
        Iterator<TestClass> it = pm.getExtent(TestClass.class).iterator();
        int n = 0;
        while (n < N/2 && it.hasNext()) {
            n++;
            it.next();
        }
        
        //modify object
        //modify all again --> move to free pages in beginning
        for (TestClass tc: pm.getExtent(TestClass.class)) {
            //modify every second element
            if (tc.getInt() %2 == 0) {
                tc.setLong(25);
            }
        }
        //commit, this should invalidate the first extent
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        try {
        	assertFalse(it.hasNext());
			//no failure here. Depending on the configuration,
			//we either get 'false' or a JDOSuerException. Both is correct.
        } catch (JDOUserException e) {
        	//good
        }

        try {
        	it.next();
        	fail();
        } catch (JDOUserException | NoSuchElementException e) {
        	//good
        }
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    /**
     * Tests extent across transaction boundaries.
     * The problem here is that extents are pos-indices, which are COWed.
     * So during a commit, object may get rewritten, changing the index,
     * meaning that the positions in the extents are wrong.
     * 
     * In this test we simply check that commits do not affect the extent if the index is not 
     * modified.
     */
    @Test
    public void testExtentAcrossCommit() {
        int N = 10000;
        PersistenceManager pm = TestTools.openPM();
        //pm.setIgnoreCache(false);
        pm.currentTransaction().begin();

        for (int i = 0; i < N; i++) {
            TestClass tc = new TestClass();
            tc.setInt(i);
            pm.makePersistent(tc);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //modify all --> create empty space in the beginning of the DB
        for (TestClass tc: pm.getExtent(TestClass.class)) {
            tc.setLong(12);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //start iterating
        Iterator<TestClass> it = pm.getExtent(TestClass.class).iterator();
        int n = 0;
        while (n < N/2 && it.hasNext()) {
            n++;
            it.next();
        }
        
        //commit, this should invalidate the first extent
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        try {
        	assertFalse(it.hasNext());
			//no failure here. Depending on the configuration,
			//we either get 'false' or a JDOSuerException. Both is correct.
        } catch (JDOUserException e) {
        	//good
        }

        try {
        	it.next();
        	fail();
        } catch (JDOUserException | NoSuchElementException e) {
        	//good
        }
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }
    
    /**
     * Tests extent across transaction boundaries.
     * The problem here is that extents are pos-indices, which are COWed.
     * So during a commit, object may get rewritten, changing the index,
     * meaning that the positions in the extents are wrong.
     * 
     * In this test, we iterate over the extent and delete previous objects, but never ahead of the
     * extent. That should work fine. 
     */
    @Test
    public void testExtentDeletionAcrossCommit() {
        int N = 10000;
        PersistenceManager pm = TestTools.openPM();
        //pm.setIgnoreCache(false);
        pm.currentTransaction().begin();

        for (int i = 0; i < N; i++) {
            TestClass tc = new TestClass();
            tc.setInt(i);
            pm.makePersistent(tc);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();
        
        //modify all --> create empty space in the beginning of the DB
        for (TestClass tc: pm.getExtent(TestClass.class)) {
            tc.setLong(12);
        }

        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //start iterating
        Iterator<TestClass> it = pm.getExtent(TestClass.class).iterator();
        int n = 0;
        int currentI = -1;
        while (it.hasNext()) {
            n++;
            TestClass tc = it.next();
            assertEquals(currentI+1, tc.getInt());
            currentI = tc.getInt();
            pm.deletePersistent(tc);
            if (n%1000==0) {
                pm.currentTransaction().commit();
                pm.currentTransaction().begin();
                break;
            }
        }
 
        try {
        	assertFalse(it.hasNext());
			//no failure here. Depending on the configuration,
			//we either get 'false' or a JDOSuerException. Both is correct.
        } catch (JDOUserException e) {
        	//good
        }

        try {
        	it.next();
        	fail();
        } catch (JDOUserException | NoSuchElementException e) {
        	//good
        }
        
        pm.currentTransaction().commit();
        pm.currentTransaction().begin();

        //Assert that not all have been removed
        it = pm.getExtent(TestClass.class).iterator();
        assertTrue(it.hasNext());
        while (it.hasNext()) {
            n++;
            TestClass tc = it.next();
            assertEquals(currentI+1, tc.getInt());
            currentI = tc.getInt();
            pm.deletePersistent(tc);
        }
        assertFalse(pm.getExtent(TestClass.class).iterator().hasNext());

        assertEquals(N, n);
        assertEquals(N, currentI+1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    /**
     * This fails during query execution with:
     * SEVERE: This iterator has been invalidated by commit() or rollback().
     */
	@Test
    public void testExtentBug1() {
		//TODO
		System.err.println("TODO Ensure that we have no index defined here!");
        int N = 100000;
        int nPost = testExtentBug(N);
		assertTrue("N="+ N + " nPost="+ nPost, N*1.1 > nPost);
		assertTrue("N="+ N + " nPost="+ nPost, N*0.9 < nPost);
    }
    
	@Test
    public void testExtentBug2() {
        TestTools.defineIndex(TestClass.class, "_int", true);
        int N = 100000;
        int nPost = testExtentBug(N);
		assertEquals(N, nPost);
	}

    @SuppressWarnings("unchecked")
	private int testExtentBug(int N) {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        for (int i = 0; i < N; i++) {
            TestClass tc = new TestClass();
            tc.setInt(i);
            pm.makePersistent(tc);
            if (i % 1000 == 0) {
            	pm.currentTransaction().commit();
            	pm.currentTransaction().begin();
            }
        }

        pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		int nPost = 0;
		int currentPostId = -1;
		boolean isFinished = false;
		while (!isFinished) {
			Query qP = pm.newQuery(TestClass.class, "_int > " + currentPostId + 
					" &&  _int <= " + (currentPostId+20000));
			Collection<TestClass> cP = (Collection<TestClass>)qP.execute();
			if (cP.isEmpty()) {
				isFinished = true;
				break;
			}
			for (TestClass p: cP) {
				nPost++;
				currentPostId = p.getInt();
				//Okay, this loop
				if (nPost % 1000 == 0) {
					//System.out.print(".");
					//System.out.println(nPost + " - " + currentPostId);
					//********************************************************
					//Commenting out the following 'break' avoids the problem.
					//********************************************************
					break;
				}
			}
			qP.closeAll();
			pm.currentTransaction().commit();
			pm.currentTransaction().begin();
		}
		System.out.println();
			
		pm.currentTransaction().commit();
		
		pm.close();
		return nPost;
    }
}
