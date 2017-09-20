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
package org.zoodb.internal.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.zoodb.api.impl.ZooPC;
import org.zoodb.internal.DataDeSerializer;
import org.zoodb.internal.GenericObject;
import org.zoodb.internal.ZooClassDef;
import org.zoodb.internal.ZooClassProxy;
import org.zoodb.internal.ZooFieldDef;
import org.zoodb.internal.ZooHandleImpl;
import org.zoodb.internal.server.index.PagedOidIndex;
import org.zoodb.internal.server.index.SchemaIndex.SchemaIndexEntry;
import org.zoodb.internal.util.CloseableIterator;
import org.zoodb.tools.DBStatistics.STATS;

public interface DiskAccess {
	
	public long[] allocateOids(int oidAllocSize);

	public CloseableIterator<ZooPC> readAllObjects(long schemaId, boolean loadFromCache);
	
	/**
	 * Locate an object.
	 * @param oid The OID of the object to read
	 * @return Path name of the object (later: position of obj)
	 */
	public ZooPC readObject(long oid);
	public ZooPC readObject(DataDeSerializer dds, long oid);
	
	public void close();

	public void commit();

	/**
	 * Defines an index and populates it. All objects are put into the cache. This is not 
	 * necessarily useful, but it is a one-off operation. Otherwise we would need a special
	 * purpose implementation of the deserializer, which would have the need for a cache removed.
	 * @param cls The class for which an index should be defined
	 * @param field The field for which an index should be defined
	 * @param isUnique Whether the index should be unique
	 */
	void defineIndex(ZooClassDef cls, ZooFieldDef field, boolean isUnique);

	public boolean removeIndex(ZooClassDef def, ZooFieldDef field);

	public Collection<ZooClassDef> readSchemaAll();

	/**
	 * WARNING: float/double values need to be converted with BitTools before used on indices.
	 * @param field Field The indexed field
	 * @param minValue range minimum
	 * @param maxValue range maximum
	 * @param loadFromCache Whether to load object from cache, if possible
	 * @return An iterator over all matching objects
	 */
	Iterator<ZooPC> readObjectFromIndex(ZooFieldDef field, 
			long minValue, long maxValue, boolean loadFromCache);

	public long getStats(STATS stats);

    public String checkDb();

	public void dropInstances(ZooClassProxy def);

	public void defineSchema(ZooClassDef def);

	public void newSchemaVersion(ZooClassDef defNew);

	public void renameSchema(ZooClassDef def, String newName);

	public void undefineSchema(ZooClassProxy def);

	public ServerResponse readObject(ZooPC pc);

	public GenericObject readGenericObject(ZooClassDef def, long oid);

	public void refreshSchema(ZooClassDef def);

	public long getObjectClass(long oid);

    public SchemaIndexEntry getSchemaIE(ZooClassDef def);

    public ObjectWriter getWriter(ZooClassDef def);

    public PagedOidIndex getOidIndex();

	public void revert();

    public CloseableIterator<ZooHandleImpl> oidIterator(ZooClassProxy px, boolean subClasses);

	public long countInstances(ZooClassProxy clsDef, boolean subClasses);

	boolean checkIfObjectExists(long oid);

	public long beginTransaction();

	public OptimisticTransactionResult rollbackTransaction();

	public OptimisticTransactionResult beginCommit(ArrayList<TxObjInfo> updates);

	public OptimisticTransactionResult checkTxConsistency(ArrayList<TxObjInfo> updates);
	
}
