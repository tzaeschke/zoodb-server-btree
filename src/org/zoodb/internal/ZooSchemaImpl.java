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
package org.zoodb.internal;

import java.util.Collection;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.client.SchemaManager;
import org.zoodb.internal.util.DBTracer;
import org.zoodb.schema.ZooClass;
import org.zoodb.schema.ZooHandle;
import org.zoodb.schema.ZooSchema;


/**
 * Public factory class to manage database schemata. In addition to SchemaManager, this class also
 * performs a number of checks, such as validity of names and session status.
 * 
 * @see ZooSchema
 * @author ztilmann
 */
public final class ZooSchemaImpl implements ZooSchema {

	private final SchemaManager sm;
	private final Session s;
	
	ZooSchemaImpl(Session s, SchemaManager sm) {
		this.sm = sm;
		this.s = s;
	}
	
	/**
	 * Define a new database class schema based on the given Java class.
	 * @param cls The Java class for which a schema should be defined
	 * @return New schema object
	 * @see ZooSchema#addClass(Class)
	 */
	@Override
	public ZooClass addClass(Class<?> cls) {
		DBTracer.logCall(this, cls);
    	checkValidity(true);
		return sm.createSchema(null, cls);
	}

	/**
	 * Locate the class definition for the given class.
	 * @param cls The Java class whose schema should be returned
	 * @return The class definition or {@code null} if the class is not defined in the database
	 * @see ZooSchema#getClass(Class)
	 */
	@Override
	public ZooClass getClass(Class<?> cls) {
		DBTracer.logCall(this, cls);
		checkValidityRead();
		return sm.locateSchema(cls, null);
	}

	/**
	 * Locate the class definition for the given class.
	 * @param className The name of the class whose schema should be returned
	 * @return The class definition or {@code null} if the class is not defined in the database
	 * @see ZooSchema#getClass(String)
	 */
	@Override
	public ZooClass getClass(String className) {
		DBTracer.logCall(this, className);
		checkValidityRead();
		return sm.locateSchema(className);
	}

	/**
	 * This declares a new database class schema. This method creates an empty class
	 * with no attributes. It does not consider any existing Java classes of the same name.  
	 * @param className Class name
	 * @return New schema object
	 * @see ZooSchema#defineEmptyClass(String)
	 */
	@Override
	public ZooClass defineEmptyClass(String className) {
		DBTracer.logCall(this, className);
    	checkValidity(true);
    	if (!checkJavaClassNameConformity(className)) {
    		throw new IllegalArgumentException("Not a valid class name: \"" + className + "\"");
    	}
		return sm.declareSchema(className, null);
	}
	
	/**
	 * Declares a new class with a given super-class. The new class contains no attributes
	 * except attributes derived from the super class. This method does not consider any existing 
	 * Java classes of the same name.  
	 * @param className Name of class
	 * @param superCls Super class, or {@code null} for none
	 * @return New schema object
	 * @see ZooSchema#defineEmptyClass(String, ZooClass)
	 */
	@Override
	public ZooClass defineEmptyClass(String className, ZooClass superCls) {
		DBTracer.logCall(this, className, superCls);
    	checkValidity(true);
    	if (!checkJavaClassNameConformity(className)) {
    		throw new IllegalArgumentException("Not a valid class name: \"" + className + "\"");
    	}
		return sm.declareSchema(className, superCls);
	}
	
	private static boolean checkJavaClassNameConformity(String className) {
		if (className == null || className.length() == 0) {
			return false;
		}
		for (int i = 0; i < className.length(); i++) {
			char c = className.charAt(i);
			if (i == 0) {
				if (!Character.isJavaIdentifierStart(c)) {
					return false;
				}
			} else {
				if (c != '.' && !Character.isJavaIdentifierPart(c)) {
					return false;
				}
			}
		}
		
		//check existing class. For now we disallow class names of non-persistent classes.
		try {
			Class<?> cls = Class.forName(className);
			if (!ZooPC.class.isAssignableFrom(cls)) {
				throw new IllegalArgumentException("Class is not persistence capable: " + cls);
			}
		} catch (ClassNotFoundException e) {
			//okay, class not found.
		}
		
		return true;
	}
	
	/**
	 * @see org.zoodb.schema.ZooSchema#getHandle(long)
	 */
	@Override
	public ZooHandle getHandle(long oid) {
		DBTracer.logCall(this, oid);
		checkValidityRead();
		return s.getHandle(oid);
	}

	/**
	 * @see org.zoodb.schema.ZooSchema#getHandle(Object)
	 */
	@Override
	public ZooHandle getHandle(Object pc) {
		DBTracer.logCall(this, pc);
		checkValidityRead();
    	if (!(pc instanceof ZooPC)) {
    		if (pc instanceof Long) {
    			return getHandle((long)pc);
    		}
			throw new IllegalArgumentException("Object is not persistence capable.");
    	}
		return s.getHandle((ZooPC)pc);
	}

	/**
 	 * @see ZooSchema#getAllClasses()
	 */
	@Override
   public Collection<ZooClass> getAllClasses() {
		DBTracer.logCall(this);
		checkValidityRead();
        return sm.getAllSchemata();
    }
    
    private void checkValidity(boolean write) {
    	if (s.isClosed()) {
    		throw new IllegalStateException("The session is closed.");
    	}
    	if (!s.isActive()) {
    		if (write || !s.getConfig().getNonTransactionalRead())
    		throw new IllegalStateException("Transaction is closed. Missing 'begin()' ?");
    	}
    }
    
    private void checkValidityRead() {
    	checkValidity(false);
    }
}
