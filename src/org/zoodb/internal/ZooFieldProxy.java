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

import org.zoodb.internal.client.SchemaManager;
import org.zoodb.schema.ZooField;
import org.zoodb.schema.ZooHandle;

/**
 * The class serves as a proxy for the latest version of a particular class in the schema version
 * tree.
 * The proxy's reference to the latest version is updated by SchemaOperations.
 * 
 * @author ztilmann
 */
public class ZooFieldProxy implements ZooField {

	private boolean isInvalid = false;
	private ZooFieldDef fieldDef;
	private final SchemaManager schemaManager;

	ZooFieldProxy(ZooFieldDef fieldDef, SchemaManager schemaManager) {
		this.fieldDef = fieldDef;
		this.schemaManager = schemaManager;
	}

	@Override
	public String toString() {
		checkInvalidRead();
		return "Class schema field: " + fieldDef.getName();
	}

	private void checkInvalidRead() {
		checkInvalid(false);
	}

	private void checkInvalidWrite() {
		checkInvalid(true);
	}

	private void checkInvalid(boolean write) {
		Session s = fieldDef.getDeclaringType().getProvidedContext().getSession();
		if (s.isClosed()) {
			throw new IllegalStateException("This schema belongs to a closed PersistenceManager.");
		}
		if (!s.isActive()) {
			if (write || !s.getConfig().getNonTransactionalRead()) {
				throw new IllegalStateException("The transaction is currently not active.");
			}
		}
		if (isInvalid) {
			throw new IllegalStateException("This schema field object is invalid, for " +
					"example because it has been deleted.");
		}
	}

	@Override
	public void remove() {
		checkInvalidWrite();
		isInvalid = true;
		fieldDef.getDeclaringType().getVersionProxy().removeField(this);
	}

	@Override
	public void rename(String fieldName) {
		checkInvalidWrite();
		ZooClassProxy.checkJavaFieldNameConformity(fieldName);
		if (fieldDef.getDeclaringType().getVersionProxy().getField(fieldName) != null) {
			throw new IllegalArgumentException("Field name already taken: " + fieldName);
		}
		schemaManager.renameField(fieldDef, fieldName);
	}

	@Override
	public String getName() {
		checkInvalidRead();
		return fieldDef.getName();
	}

	public ZooFieldDef getFieldDef() {
		return fieldDef;
	}

	public void updateVersion(ZooFieldDef newFieldDef) {
		fieldDef = newFieldDef;
	}

    @Override
    public Object getValue(ZooHandle hdl) {
		checkInvalidRead();
		ZooHandleImpl h = checkHandle(hdl);
		h.getGenericObject().activateRead();
        return h.getGenericObject().getField(fieldDef);
    }
    
    @Override
    public void setValue(ZooHandle hdl, Object val) {
		checkInvalidWrite();
		ZooHandleImpl h = checkHandle(hdl);
        h.getGenericObject().jdoZooMarkDirty();
        h.getGenericObject().setField(fieldDef, val);
    }

    private ZooHandleImpl checkHandle(ZooHandle hdl) {
    	ZooHandleImpl hdlI = (ZooHandleImpl) hdl;
    	ZooClassProxy c = (ZooClassProxy) hdlI.getType();
    	if (!fieldDef.getDeclaringType().isSuperTypeOf(c.getSchemaDef())) {
    		throw new IllegalArgumentException("Field '" + fieldDef.getName() + 
    				"' is not present in " + c.getSchemaDef().getClassName());
    	}
    	if (hdlI.getGenericObject().jdoZooIsDeleted()) {
    		throw new IllegalStateException("The handle has been deleted.");
    	}
    	return hdlI;
    }
    
	@Override
	public String getTypeName() {
		checkInvalidRead();
		return fieldDef.getTypeName();
	}

	@Override
	public void createIndex(boolean isUnique) {
		checkInvalidWrite();
		schemaManager.defineIndex(fieldDef, isUnique);
	}

	@Override
	public boolean removeIndex() {
		checkInvalidWrite();
		return schemaManager.removeIndex(fieldDef);
	}

	@Override
	public boolean hasIndex() {
		checkInvalidRead();
		return schemaManager.isIndexDefined(fieldDef);
	}

	@Override
	public boolean isIndexUnique() {
		checkInvalidRead();
		return schemaManager.isIndexUnique(fieldDef);
	}

	@Override
	public int getArrayDim() {
		return fieldDef.getArrayDim();
	}

	public void invalidate() {
		isInvalid = true;
	}
}
