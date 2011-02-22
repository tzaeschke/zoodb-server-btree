package org.zoodb.jdo.internal;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import org.zoodb.jdo.internal.client.CachedObject;
import org.zoodb.jdo.internal.model1p.Node1P;
import org.zoodb.jdo.spi.PersistenceCapableImpl;

/**
 * ZooClassDef represents a class schema definition used by the database. 
 * The highest stored schema is that of PersistenceCapableImpl.
 * 
 * @author Tilmann Z�schke
 */
public class ZooClassDef {

	private long _oid;
	private String _className;
	private transient Class _cls;
	
	private long _oidSuper;
	private String _superName;
	private transient ZooClassDef _super;
	private transient ISchema _apiHandle = null;
	
	private final List<ZooFieldDef> _fields = new LinkedList<ZooFieldDef>();
	
	public ZooClassDef(Class cls, long oid, ZooClassDef defSuper) {
		_oid = oid;
		_className = cls.getName();
		_cls = cls;
		
		if (defSuper == null && cls != PersistenceCapableImpl.class) {
			throw new IllegalStateException("No super class found: " + 
					cls.getName());
		}

		//normal class
		if (defSuper != null) {
			_superName = defSuper.getClassName();
			_super = defSuper;
			_oidSuper = _super.getOid();
		} else { //PersistenceCapableImpl
			_superName = null;
			_super = null;
			_oidSuper = 0;
		}
		
		
		//Fields:
		//TODO does this return only local fields. Is that correct? -> Information units.
		Field[] fields = _cls.getDeclaredFields(); 
		for (int i = 0; i < fields.length; i++) {
			Field jField = fields[i];
			if (Modifier.isStatic(jField.getModifiers()) || 
					Modifier.isTransient(jField.getModifiers())) {
				continue;
			}
			Class<?> jType = jField.getType();
			String fName = jField.getName();
			//we cannot set references to other ZooClassDefs yet, as they may not be made persistent 
			//yet
			ZooFieldDef zField = new ZooFieldDef(fName, jType);
			_fields.add(zField);
		}		
	}
	
	
	public void constructFields(Node1P node, List<CachedObject.CachedSchema> cachedSchemata) {
		//Fields:
		for (ZooFieldDef zField: _fields) {
			String typeName = zField.getTypeName();
			
			if (zField.isPrimitiveType()) {
				//no further work for primitives
				continue;
			}
			
			ZooClassDef typeDef = null;
			
			for (CachedObject.CachedSchema cs: cachedSchemata) {
				if (cs.getSchema().getSchemaClass().getName().equals(typeName)) {
					typeDef = cs.getSchema();
					break;
				}
			}
			
			if (typeDef==null) {
				//found SCO
			}
			
			//TODO what is this good for?
		}
	}
	
	public String getClassName() {
		return _className;
	}

	public long getOid() {
		return _oid;
	}
	
	public Class getSchemaClass() {
		return _cls;
	}

	/**
	 * 
	 * @return name or ""
	 */
	public String getSuperClassName() {
		return _superName == null? "" : _superName;
	}


	public ZooFieldDef[] getFields() {
		return _fields.toArray(new ZooFieldDef[_fields.size()]);
	}

	public ISchema getApiHandle() {
		return _apiHandle;
	}
	
	public void setApiHandle(ISchema handle) {
		_apiHandle = handle;
	}
}
