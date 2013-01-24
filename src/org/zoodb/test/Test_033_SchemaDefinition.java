/*
 * Copyright 2009-2012 Tilmann Z�schke. All rights reserved.
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
package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import javax.jdo.Extent;
import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooClass;
import org.zoodb.jdo.api.ZooField;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.jdo.internal.ZooClassDef;
import org.zoodb.test.util.TestTools;

public class Test_033_SchemaDefinition {

	private static final int SCHEMA_COUNT = 5; //Schema count on empty database
	
	@AfterClass
	public static void tearDown() {
	    TestTools.closePM();
	}

	@Before
	public void before() {
		//TestTools.removeDb();
		TestTools.createDb();
	}
	
	@After
	public void after() {
		TestTools.closePM();
		TestTools.removeDb();
	}

	
	@Test
	public void testDeclareCommit() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s);
		assertEquals("MyClass", s.getClassName());
		ZooClass s2 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s2);
		pm.currentTransaction().commit();
		
		pm.currentTransaction().begin();
		ZooClass s3 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s3);
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s4 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s4);
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testDeclareAbort() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s);
		assertEquals("MyClass", s.getClassName());
		pm.currentTransaction().rollback();
		
		//try again
		pm.currentTransaction().begin();
		ZooClass s2 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s2);
		ZooClass s3 = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s3);
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s4 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s4);
		ZooClass s5 = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s5);
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	
	@Test
	public void testDeclareFails() {
		TestTools.defineSchema(TestClassTiny.class);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		try {
			ZooSchema.declareClass(pm, TestClassTiny.class.getName());
			fail();
		} catch (JDOUserException e) {
			//good, class exists
		}
		try {
			ZooSchema.declareClass(pm, "");
			fail();
		} catch (IllegalArgumentException e) {
			//good, bad name
		}
		try {
			ZooSchema.declareClass(pm, "1342dfs");
			fail();
		} catch (IllegalArgumentException e) {
			//good, bad name
		}
		try {
			ZooSchema.declareClass(pm, null);
			fail();
		} catch (IllegalArgumentException e) {
			//good, bad name
		}
		try {
			ZooSchema.declareClass(pm, String.class.getName());
			fail();
		} catch (IllegalArgumentException e) {
			//good, non-pers
		}

		TestTools.closePM();
		
		try {
			ZooSchema.declareClass(pm, TestClassTiny2.class.getName());
			fail();
		} catch (IllegalStateException e) {
			//good, outside session
		}
	}

	
	@Test
	public void testDeclareHierarchy() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.declareClass(pm, cName1, stt);
		s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertEquals(stt, s1.getSuperClass());
		assertEquals(s1, s2.getSuperClass());
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveClassRollback() {
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass s1 = ZooSchema.declareClass(pm, "MyClass");
		s1.remove();
		
		ZooClass s2 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s2);
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		ZooClass s3 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s3);
		ZooClass s4 = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s4);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s5 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s5);
		s5.remove();

		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		ZooClass s6 = ZooSchema.locateClass(pm, "MyClass");
		assertNotNull(s6);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	public void testRemoveClassCommit() {
		PersistenceManager pm = TestTools.openPM();
		
		//delete uncommitted
		pm.currentTransaction().begin();
		ZooClass s1 = ZooSchema.declareClass(pm, "MyClass");
		s1.remove();

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		//delete committed
		ZooClass s3 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s3);
		ZooClass s4 = ZooSchema.declareClass(pm, "MyClass");
		assertNotNull(s4);
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s5 = ZooSchema.locateClass(pm, "MyClass");
		s5.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		ZooClass s6 = ZooSchema.locateClass(pm, "MyClass");
		assertNull(s6);

		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveFails() {
		TestTools.defineSchema(TestClassTiny.class);
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();

		//remove uncommitted
		ZooClass s1 = ZooSchema.declareClass(pm, "MyClass");
		s1.remove();
		try {
			s1.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		//remove committed
		ZooClass s2 = ZooSchema.declareClass(pm, "MyClass2");
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		s2 = ZooSchema.locateClass(pm, "MyClass2");
		s2.remove();
		try {
			s2.remove();
			fail();
		} catch (JDOUserException e) {
			//good
		}

		pm.currentTransaction().commit();
		pm.currentTransaction().begin();

		s2 = ZooSchema.locateClass(pm, "MyClass2");
		assertNull(s2);

		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	
	@Test
	public void testRemoveHierarchy() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		s1.remove();
		assertNull(ZooSchema.declareClass(pm, cName1));
		assertNull(ZooSchema.declareClass(pm, cName2));
		
		pm.currentTransaction().rollback();
		
		//try again, this time with commit
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.declareClass(pm, cName1, stt);
		s2 = ZooSchema.declareClass(pm, cName2, s1);
		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		stt.remove();
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertNull(s1);
		assertNull(s2);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		assertNull(stt);
		assertNull(s1);
		assertNull(s2);
		
		pm.currentTransaction().commit();
		TestTools.closePM();
	}

	@Test
	public void testLocateClass() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		assertNull(ZooSchema.locateClass(pm, String.class));
		assertNull(ZooSchema.locateClass(pm, (Class<?>)null));
		assertNull(ZooSchema.locateClass(pm, (String)null));
		assertNull(ZooSchema.locateClass(pm, ""));
		assertNull(ZooSchema.locateClass(pm, "  %% "));
		
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertTrue(stt == ZooSchema.locateClass(pm, TestClassTiny.class));
		assertTrue(s1 == ZooSchema.locateClass(pm, cName1));
		assertTrue(s2 == ZooSchema.locateClass(pm, cName2));
		
		pm.currentTransaction().rollback();
		
		try {
			ZooSchema.declareClass(pm, cName1, stt);
			fail();
		} catch(IllegalStateException e) {
			//good, pm is closed!
		}
		
		TestTools.closePM();
		
		try {
			ZooSchema.declareClass(pm, cName1, stt);
			fail();
		} catch(IllegalStateException e) {
			//good, pm is closed!
		}
	}	
	
	
	@Test
	public void testGetAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		assertTrue(s1.getAllFields().size() == 2);
		assertTrue(s1.getLocalFields().size() == 0);
		assertTrue(s2.getAllFields().size() == 2);
		assertTrue(s2.getLocalFields().size() == 0);
		
		assertNull(s1.locateField("_int1"));
		assertNotNull(s1.locateField("_int"));
		assertNotNull(s2.locateField("_long"));
		
		s1.declareField("_int1", Integer.TYPE);
		s1.declareField("_long1", Long.TYPE);
		s2.declareField("ref1", s1, 0);
		s2.declareField("ref1Array", s1, 2);

		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		//check all fields 
		checkFields(s1.getAllFields(), "_int", "_long", "_int1", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int1", "_long1", "ref1", "ref1Array");		
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		//check 1st class
		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");

		//check all fields 
		checkFields(s1.getAllFields(), "_int", "_long", "_int1", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int1", "_long1", "ref1", "ref1Array");		

		pm.currentTransaction().commit();
		TestTools.closePM();
		
		try {
			s1.getAllFields();
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
		try {
			s1.getLocalFields();
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	
	@Test
	public void testAddAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		
		s1.declareField("_int1", Integer.TYPE);
		s1.declareField("_long1", Long.TYPE);
		s2.declareField("ref1", s1, 0);
		s2.declareField("ref1Array", s1, 2);

		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");

		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}

	@Test
	public void testAddAttributeFails() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		
		s1.declareField("_int1", Integer.TYPE);
		s1.declareField("_long1", Long.TYPE);
		s2.declareField("ref1", s1, 0);
		s2.declareField("ref1Array", s1, 2);

		try {
			s1.declareField("_long", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is already taken...
		}
		try {
			s1.declareField("_long1", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is already taken...
		}
		try {
			s1.declareField(null, Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			s1.declareField("", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			s1.declareField("1_long1", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			s1.declareField("MyClass.x", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}

		try {
			s1.declareField("1_long1", null);
			fail();
		} catch (IllegalArgumentException e) {
			//good, this type is invalid...
		}

		
		//check local fields
		checkFields(s1.getLocalFields(), "_int1", "_long1");
		checkFields(s2.getLocalFields(), "ref1", "ref1Array");
		
		pm.currentTransaction().commit();

		try {
			s1.declareField("xyz", Long.TYPE);
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
		
		TestTools.closePM();

		try {
			s1.declareField("xyz2", Long.TYPE);
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	@Test
	public void testRenameAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		
		ZooField f11 = s1.declareField("_int1", Integer.TYPE);
		ZooField f12 = s1.declareField("_long1", Long.TYPE);
		ZooField f21 = s2.declareField("ref1", s1, 0);
		ZooField f22 = s2.declareField("ref1Array", s1, 2);

		f11.rename("_int11");
		try {
			f12.rename("_long");
		} catch (IllegalArgumentException e) {
			//good, this name is already taken...
		}
		try {
			f21.rename("");
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		try {
			f22.rename("123_dhsak");
		} catch (IllegalArgumentException e) {
			//good, this name is invalid...
		}
		
		//check local fields
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");
		
		pm.currentTransaction().commit();
		
		//try again
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		//check local fields
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");

		s1.locateField("_int11").rename("_int111");
		checkFields(s1.getLocalFields(), "_int111", "_long1");
		
		//rollback
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();

		//check again
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_int11", "_long1", "ref1", "ref1Array");
		
		pm.currentTransaction().rollback();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		checkFields(s1.getLocalFields(), "_int11", "_long1");
		checkFields(s2.getAllFields(), "_int11", "_long1", "ref1", "ref1Array");
		pm.currentTransaction().rollback();
		TestTools.closePM();
		
		try {
			s1.declareField("xyz", Long.TYPE);
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	@Test
	public void testRemoveAttribute() {
		TestTools.defineSchema(TestClassTiny.class);
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		System.out.println("List: " + s1.getLocalFields().size());
		System.out.println("List: " + s1.getAllFields().size());
		System.out.println("List: " + s1.getAllFields());
		
		try {
			s2.declareField("_int", Long.TYPE);
			fail();
		} catch (IllegalArgumentException e) {
			// good, field already exists in super-super class
		}
		
		ZooField f11 = s1.declareField("_int1", Integer.TYPE);
		ZooField f12 = s1.declareField("_long1", Long.TYPE);
		ZooField f13 = s1.declareField("_long12", Long.TYPE);
		ZooField f21 = s2.declareField("ref1", s1, 0);
		ZooField f22 = s2.declareField("ref1Array", s1, 1);
		assertNotNull(f13);
		assertNotNull(f22);

		f11.remove();
		s1.removeField(s1.locateField("_long12"));
		s2.removeField(f21.getFieldName());
		
		List<ZooField> fields1 = s1.getLocalFields();
		assertTrue(fields1.get(0).getFieldName() == "_long1");
		assertEquals(1, fields1.size());
		
		List<ZooField> fields2 = s2.getLocalFields();
		assertTrue(fields2.get(0).getFieldName() == "ref1Array");
		assertEquals(1, fields2.size());

		checkFields(s1.getLocalFields(), "_long1");
		checkFields(s2.getLocalFields(), "ref1Array");
		
		checkFields(s1.getAllFields(), "_int", "_long", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_long1", "ref1Array");

		pm.currentTransaction().commit();
		TestTools.closePM();

		//load and check again
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		s2 = ZooSchema.locateClass(pm, cName2);
		
		checkFields(s1.getAllFields(), "_int", "_long", "_long1");
		checkFields(s2.getAllFields(), "_int", "_long", "_long1", "ref1Array");

		pm.currentTransaction().rollback();
		TestTools.closePM();
		try {
			f12.remove();
			fail();
		} catch (IllegalStateException e) {
			//good, pm is closed
		}
	}

	@Test
	public void testSchemaCountAbort() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 0);
		
		ZooClass stt = ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		
		ZooField f1 = s2.declareField("_int1", Integer.TYPE);
		f1.rename("_int1_1");
		f1.remove();
		
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 0);
		
		pm.currentTransaction().rollback();
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaCountCommit() {
		String cName1 = "MyClassA";
		String cName2 = "MyClassB";
		
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		ZooClass stt = ZooSchema.defineClass(pm, TestClassTiny.class);
		ZooClass s1 = ZooSchema.declareClass(pm, cName1, stt);
		ZooClass s2 = ZooSchema.declareClass(pm, cName2, s1);
		
		ZooField f1 = s1.declareField("_long1", Long.TYPE);
		f1.rename("_long_1_1");
		
		ZooField f2 = s2.declareField("_int1", Integer.TYPE);
		f2.rename("_int1_1");
		f2.remove();
		s2.remove();
		
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		
		checkSchemaCount(pm, 2);

		//test modify super-class
		stt = ZooSchema.locateClass(pm, TestClassTiny.class);
		s1 = ZooSchema.locateClass(pm, cName1);
		stt.declareField("xyz", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 4);  //class and sub-class have new attribute

		//test add
		s1 = ZooSchema.locateClass(pm, cName1);
		s1.declareField("xyz2", Long.TYPE);
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 5);  //class and sub-class have new attribute

		//test rename
		s1 = ZooSchema.locateClass(pm, cName1);
		s1.locateField("xyz2").rename("xyz3");
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 6);  //class and sub-class have new attribute

		//test remove
		s1 = ZooSchema.locateClass(pm, cName1);
		s1.locateField("xyz3").remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 7);  //class and sub-class have new attribute

		//test combo (should result in one change only)
		s1 = ZooSchema.locateClass(pm, cName1);
		f1 = s1.declareField("aaa", Long.TYPE);
		f1.rename("aaa2");
		f2 = s1.declareField("bbb", Long.TYPE);
		f2.rename("bbb2");
		f2.remove();
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
		checkSchemaCount(pm, 8);  //class and sub-class have new attribute

		
		TestTools.closePM();
	}
	
	private void checkFields(List<ZooField> list, String ...names) {
		for (int i = 0; i < names.length; i++) {
			assertEquals(names[i], list.get(i).getFieldName());
		}
		assertEquals(names.length, list.size());
	}
	
	private void checkSchemaCount(PersistenceManager pm, int expected) {
		Extent<?> e = pm.getExtent(ZooClassDef.class);
		int n = 0;
		for (Object o: e) {
			assertNotNull(o);
			n++;
		}
		assertEquals(SCHEMA_COUNT + expected, n);
	}
}
