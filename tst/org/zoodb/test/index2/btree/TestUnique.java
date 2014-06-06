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

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;

public class TestUnique {

    private BTreeBufferManager bufferManager = new BTreeMemoryBufferManager();

    @Test
    public void testSameKeyPair() {
        UniquePagedBTree tree = (UniquePagedBTree) factory().getTree();
        tree.insert(1, 1);
        tree.insert(1, 2);
        System.out.println(tree);
        tree.insert(1, 3);
        System.out.println(tree);
        assertEquals(new Long(3), tree.search(1));
    }

    private BTreeFactory factory() {
        return new BTreeFactory(bufferManager, true);
    }

}
