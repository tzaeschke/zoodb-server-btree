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
package org.zoodb;

import java.util.Collection;

import javax.jdo.PersistenceManager;

import org.zoodb.jdo.ZooJdoHelper;
import org.zoodb.tools.ZooHelper;

/**
 * The main ZooDB rolling session class. Rolling sessions don't need to call {@code begin()}
 * after {@code commit()} or {@code rollback()}. Instead there is always an open transaction.
 * Note that while this can be very convenient, it may affect server performance in concurrent
 * scenarios.
 * 
 * @author ztilmann
 * @deprecated Currently not supported, please use JDO instead
 */
public class ZooRollingSession {

	//TODO: The concurrency aspect could be improved by internally starting the session when the 
	//first object is accessed or when the first API method is called, such as newQuery(). 

	
	//private final Session tx;
	private final PersistenceManager pm;
	
	private ZooRollingSession(String dbName) {
		pm = ZooJdoHelper.openDB(dbName);
		pm.currentTransaction().begin();
	}
	
	/**
	 * Opens the database at the given location. 
	 * The database is created if it does not exist.
	 * By default databases are created in %USER_HOME%/zoodb. 
	 * 
	 * @param dbName
	 * @return ZooRollingSession object
	 */
	static final ZooRollingSession open(String dbName) {
        if (!ZooHelper.dbExists(dbName)) {
            // create database
            // By default, all database files will be created in %USER_HOME%/zoodb
            ZooHelper.createDb(dbName);
        }
		return new ZooRollingSession(dbName);
	}
	
	public void commit() {
		pm.currentTransaction().commit();
		pm.currentTransaction().begin();
	}
	
	public void rollback() {
		pm.currentTransaction().rollback();
		pm.currentTransaction().begin();
	}
	
	public void makePersistent(Object pc) {
		pm.makePersistent(pc);
	}
	
	public void delete(Object pc) {
		pm.deletePersistent(pc);
	}
	
	public void close() {
		if (pm.currentTransaction().isActive()) {
			pm.currentTransaction().rollback();
		}
		pm.close();
		pm.getPersistenceManagerFactory().close();
	}
	
	public Collection<?> query(String query) {
		return (Collection<?>) pm.newQuery(query).execute();
	}
}

