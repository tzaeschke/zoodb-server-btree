/*
 * Copyright 2009-2015 Tilmann Zaeschke. All rights reserved.
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.test.testutil.TestTools;

/**
 * Tests for query setOrdering().
 * 
 * @author ztilmann
 *
 */
public class Test_127_QueryBoolFunctions {

	@BeforeClass
	public static void setUp() {
        TestTools.removeDb();
		TestTools.createDb();
		TestTools.defineSchema(TestClass.class);
	}

	@Before
	public void before() {
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();

        pm.newQuery(TestClass.class).deletePersistentAll();
        
        TestClass tc1 = new TestClass();
        tc1.setData(1, false, 'c', (byte)127, (short)32001, 1234567890L, "xyz5", new byte[]{1,2},
        		-1.1f, 35);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12, false, 'd', (byte)126, (short)32002, 1234567890L, "xyz4", new byte[]{1,2},
        		-0.1f, 34);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(123, false, 'x', (byte)125, (short)32003, 1234567891L, "xyz1", new byte[]{1,2},
        		0.1f, 3.0);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(1234, false, 'f', (byte)124, (short)32004, 1234567890L, "xyz2", new byte[]{1,2},
        		1.1f, -0.01);
        pm.makePersistent(tc1);
        tc1 = new TestClass();
        tc1.setData(12345, false, 'g', (byte)123, (short)32005, 1234567890L, "xyz3", new byte[]{1,2},
        		11.1f, -35);
        pm.makePersistent(tc1);
        
        pm.currentTransaction().commit();
        TestTools.closePM();;
	}
		
	@After
	public void afterTest() {
		TestTools.closePM();
	}
	
	@AfterClass
	public static void tearDown() {
		TestTools.removeDb();
	}

	@Test
	public void testBoolFunctionFail() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		checkSetFilterFails(pm, "isEmpty");
		checkSetFilterFails(pm, "isEmpty == 3");
		checkSetFilterFails(pm, "isEmpty()");
		
		checkSetFilterFails(pm, "startsWith('asc')");
		
		checkSetFilterFails(pm, "_int.isEmpty()");

		checkSetFilterFails(pm, "_string.isEmpty()");

		checkSetFilterFails(pm, "_string.startsWith");
		checkSetFilterFails(pm, "_string.startsWith()");
		checkSetFilterFails(pm, "_string.startsWith(1)");
		checkSetFilterFails(pm, "_string.startsWith('z', 'b')");
		checkSetFilterFails(pm, "_string.startsWith('z').startsWith('x')");
		
		TestTools.closePM();
	}
	
	private void checkSetFilterFails(PersistenceManager pm, String s) {
		Query q1 = pm.newQuery(TestClass.class);
		try {
			q1.setOrdering(s);
			q1.execute();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
		
		try {
			Query q2 = pm.newQuery(TestClass.class, "order by " + s);
			q2.execute();
			fail();
		} catch (JDOUserException e) {
			//good, we got an JDOUSerException()
		}
	}
	
	@Test
	public void testString() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		Query q = null; 
		
		q = pm.newQuery(TestClass.class);
		q.setFilter("_string.matches('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.matches('xyz')");
		checkString(q);

		q.setFilter("_string.matches('.*3.*')");
		checkString(q, "xyz3");

		q.setFilter("_string.matches('.*y.*')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.startsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.startsWith('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

		q.setFilter("_string.startsWith('xyz12')");
		checkString(q);

		q.setFilter("_string.endsWith('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.endsWith('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.endsWith('xyz12')");
		checkString(q);

		//non-JDO:
		q.setFilter("_string.contains('xyz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.contains('yz1')");
		checkString(q, "xyz1");

		q.setFilter("_string.contains('xyz12')");
		checkString(q);

		q.setFilter("_string.contains('xyz')");
		checkString(q, "xyz1", "xyz2", "xyz3", "xyz4", "xyz5");

	}
	
    @SuppressWarnings("unchecked")
	private void checkString(Query q, String ... matches) {
    	Collection<TestClass> c = (Collection<TestClass>) q.execute(); 
		for (int i = 0; i < matches.length; i++) {
			boolean match = false;
			for (TestClass t: c) {
				if (t.getString().equals(matches[i])) {
					match = true;
					break;
				}
			}
			assertTrue(match);
		}
		assertEquals(matches.length, c.size());
	}

	
    @Test
    public void testList() {
		//TODO test List functions
		System.err.println("TODO Test_127.testList()");
		fail();
    }
	
    @Test
    public void testMap() {
		//TODO test Map functions
		System.err.println("TODO Test_127.testMap()");
		fail();
    }
	
    @Test
    public void testCollections() {
		//TODO test Collection functions
		System.err.println("TODO Test_127.testCollections()");
		fail();
    }
	
}
