/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.test.index2.btree;

import java.util.Arrays;

import org.junit.Test;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.tools.ZooConfig;

public class BTreeFactoryTest {

	@Test
	public void testFactory() {
		StorageChannel storage = new StorageRootInMemory(ZooConfig.getFilePageSize());
		boolean isUnique = true;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(storage, isUnique);
		
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));

		System.out.println(getTestTree());
		
	}
	public static PagedBTree getTestTree() {
		StorageChannel storage = new StorageRootInMemory(ZooConfig.getFilePageSize());
		boolean isUnique = true;
		BTreeStorageBufferManager bufferManager = new BTreeStorageBufferManager(storage, isUnique);
		
		BTreeFactory factory = new BTreeFactory(bufferManager, true);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		return tree;
	}
}
