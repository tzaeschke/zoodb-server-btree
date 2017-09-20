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
package org.zoodb.tools;

import java.lang.reflect.Constructor;

import org.zoodb.internal.util.DBLogger;
import org.zoodb.jdo.ZooJdoProperties;
import org.zoodb.tools.impl.DataStoreManager;

public class ZooHelper {

	private static DataStoreManager INSTANCE = null;

	public static DataStoreManager getDataStoreManager() {
		if (INSTANCE != null && ZooConfig.getFileManager().equals(INSTANCE.getClass().getName())) {
			return INSTANCE;
		}
		
		//create a new one
		try {
			Class<?> cls = Class.forName(ZooConfig.getFileManager());
			Constructor<?> con = cls.getConstructor();
			DataStoreManager dsm = (DataStoreManager) con.newInstance();
			INSTANCE = dsm;
			return dsm;
		} catch (Exception e) {
			throw DBLogger.newFatal("", e);
		}
	}
	
	/**
	 * Create a database file.
	 * 
	 * If only a file name is given, the database will create in %USER_HOME%/zoodb on Windows 
	 * or ~/zoodb on Linux/UNIX.
	 * 
	 * If a full path is given, the full path will be used instead.
	 * 
     * Any necessary parent folders are created automatically.

     * It is recommended to use <code>.zdb</code> as file extension, for example 
     * <code>myDatabase.zdb</code>.
     * 
	 * @param dbName The database name or path
	 * @see ZooJdoProperties#ZooJdoProperties(String)
	 * @see DataStoreManager#createDb(String)
	 */
	public static void createDb(String dbName) {
		getDataStoreManager().createDb(dbName);
	}
	
	/**
	 * Check if a database exists. This checks only whether the file exists, not whether it is a 
	 * valid database file.
	 * 
	 * @param dbName The database name or path
	 * @return <code>true</code> if the database exists.
	 * @see DataStoreManager#dbExists(String)
	 */
    public static boolean dbExists(String dbName) {
    	return getDataStoreManager().dbExists(dbName);
    }

    /**
     * Delete a database(-file).
     * @param dbName The database name or path
     * @return {@code true} if the database could be removed, otherwise false
	 * @see DataStoreManager#removeDb(String)
     */
    public static boolean removeDb(String dbName) {
    	return getDataStoreManager().removeDb(dbName);
    }
}
