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
package org.zoodb.internal.server.index;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;

public class IndexFactory {

	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @return a new index
	 */
	public static LongLongIndex createIndex(PAGE_TYPE type, IOResourceProvider storage) {
		//  return new PagedLongLong(type, storage);
		return new BTreeIndexNonUnique(type, storage);
	}

	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex loadIndex(PAGE_TYPE type, IOResourceProvider storage, int pageId) {
		//         return new PagedLongLong(type, storage, pageId);
		return new BTreeIndexNonUnique(type, storage, pageId);
	}

	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @return a new index
	 */
	public static LongLongIndex.LongLongUIndex createUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage) {
		//         return new PagedUniqueLongLong(type, storage);
		return new BTreeIndexUnique(type, storage);
	}

	/**
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param pageId page id of the root page
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex.LongLongUIndex loadUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage, int pageId) {
		//         return new PagedUniqueLongLong(type, storage, pageId);
		return new BTreeIndexUnique(type, storage, pageId);
	}

	/**
	 * EXPERIMENTAL! Index that has bit width of key and value as parameters.
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param keySize The number of bytes required by the key
	 * @param valSize The number of bytes required by the value
	 * @return a new index
	 */
	public static LongLongIndex.LongLongUIndex createUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage, int keySize, int valSize) {
		//         return new PagedUniqueLongLong(type, storage, keySize, valSize);
		return new BTreeIndexUnique(type, valSize, storage);
	}

	/**
	 * EXPERIMENTAL! Index that has bit width of key and value as parameters.
	 * @param type The page type for index pages
	 * @param storage The output stream
	 * @param pageId page id of the root page
	 * @param keySize The number of bytes required by the key
	 * @param valSize The number of bytes required by the value
	 * @return an index reconstructed from disk
	 */
	public static LongLongIndex.LongLongUIndex loadUniqueIndex(PAGE_TYPE type, 
			IOResourceProvider storage, int pageId, int keySize, int valSize) {
		//         return new PagedUniqueLongLong(type, storage, pageId, keySize, valSize);

		return new BTreeIndexUnique(type, valSize, storage, pageId);
	}

}
