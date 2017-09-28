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
package org.zoodb.spi;

import org.zoodb.internal.server.index.BTreeIndexNonUnique;
import org.zoodb.internal.server.index.BTreeIndexUnique;
import org.zoodb.internal.server.index.IndexFactory;

public class BTreePluginInitializer {

	public static void activate() {
		IndexFactory.CREATE_INDEX = BTreeIndexNonUnique::new;
		IndexFactory.LOAD_INDEX = BTreeIndexNonUnique::new;
		IndexFactory.CREATE_UNIQUE_INDEX = BTreeIndexUnique::new;
		IndexFactory.LOAD_UNIQUE_INDEX = BTreeIndexUnique::new;

		IndexFactory.CREATE_UNIQUE_INDEX_SIZED = (pt, io, keySize, valSize) -> (new BTreeIndexUnique(pt,  io));
		IndexFactory.LOAD_UNIQUE_INDEX_SIZED = 
				(pt, io, pageId, keySize, valSize) -> (new BTreeIndexUnique(pt,  io, pageId));
	}
}
