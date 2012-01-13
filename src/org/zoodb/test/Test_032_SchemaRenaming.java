package org.zoodb.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import javax.jdo.JDOUserException;
import javax.jdo.PersistenceManager;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.util.TestTools;

public class Test_032_SchemaRenaming {

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
	public void testRenameFailsIfExists() {
		TestTools.defineSchema(TestClassTiny.class);
		TestTools.defineSchema(TestClassTinyClone.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooSchema.locate(pm, TestClassTiny.class.getName());
		
		try {
			s.rename(TestClassTinyClone.class.getName());
			fail();
		} catch (JDOUserException e) {
			//good
		}
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaRenameWithoutInstances() {
		TestTools.defineSchema(TestClassTiny.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooSchema.locate(pm, TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaRenameWithInstances() {
		TestTools.defineSchema(TestClassTiny.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClassTiny pc = new TestClassTiny();
		pc.setInt(1234);
		pm.makePersistent(pc);
		Object oid = pm.getObjectId(pc);
		pm.currentTransaction().commit();
		
		TestTools.closePM();

		
		//rename
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooSchema.locate(pm, TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		Object pc2 = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, pc2.getClass());
		assertEquals(1234, ((TestClassTinyClone)pc2).getInt());
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pc2 = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, pc2.getClass());
		assertEquals(1234, ((TestClassTinyClone)pc2).getInt());
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pc2 = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, pc2.getClass());
		assertEquals(1234, ((TestClassTinyClone)pc2).getInt());
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	private void checkRename(Class<?> oldCls, Class<?> newCls, PersistenceManager pm) {
		assertNotNull(ZooSchema.locate(pm, newCls.getName()));
		assertNull(ZooSchema.locate(pm, oldCls.getName()));
		assertNotNull(ZooSchema.locate(pm, newCls));
		assertNull(ZooSchema.locate(pm, oldCls));
	}
		
	
	@Test
	public void testSchemaRenameWithSubClasses() {
		TestTools.defineSchema(TestClassTiny.class);
		TestTools.defineSchema(TestClassTiny2.class);

		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		TestClassTiny pc = new TestClassTiny();
		pc.setInt(1234);
		pm.makePersistent(pc);
		Object oid = pm.getObjectId(pc);
		TestClassTiny2 pc2 = new TestClassTiny2();
		pc2.setInt(1234);
		pm.makePersistent(pc2);
		Object oid2 = pm.getObjectId(pc2);
		pm.currentTransaction().commit();
		
		TestTools.closePM();

		
		//rename
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooSchema.locate(pm, TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		ZooSchema s2 = ZooSchema.locate(pm, TestClassTiny2.class.getName());
		s2.rename(TestClassTinyClone2.class.getName());

		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		checkRename(TestClassTiny2.class, TestClassTinyClone2.class, pm);
		Object obj = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, obj.getClass());
		assertEquals(1234, ((TestClassTinyClone)obj).getInt());
		Object obj2 = pm.getObjectById(oid2);
		assertEquals(TestClassTinyClone2.class, obj2.getClass());
		assertEquals(1234, ((TestClassTinyClone2)obj2).getInt());
		pm.currentTransaction().commit();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		checkRename(TestClassTiny2.class, TestClassTinyClone2.class, pm);
		obj = pm.getObjectById(oid);
		assertEquals(TestClassTinyClone.class, obj.getClass());
		assertEquals(1234, ((TestClassTinyClone)obj).getInt());
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
	
	@Test
	public void testSchemaRenameRollback() {
		TestTools.defineSchema(TestClassTiny.class);
		
		//rename
		PersistenceManager pm = TestTools.openPM();
		pm.currentTransaction().begin();
		
		ZooSchema s = ZooSchema.locate(pm, TestClassTiny.class.getName());
		s.rename(TestClassTinyClone.class.getName());
		//check before commit
		checkRename(TestClassTiny.class, TestClassTinyClone.class, pm);
		pm.currentTransaction().rollback();
		
		//check after commit
		pm.currentTransaction().begin();
		checkRename(TestClassTinyClone.class, TestClassTiny.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
		
		//check new session
		pm = TestTools.openPM();
		pm.currentTransaction().begin();
		checkRename(TestClassTinyClone.class, TestClassTiny.class, pm);
		pm.currentTransaction().commit();
		
		TestTools.closePM();
	}
}
