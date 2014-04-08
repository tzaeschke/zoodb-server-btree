package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeLeafIterator;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

public class BTreeIteratorTest {

	@Test(expected = NoSuchElementException.class)
	public void testLeafIterate() {
		BTree<PagedBTreeNode> tree = TestBTree.getTestTree(new BTreeMemoryBufferManager());
		System.out.println(tree);
		
		BTreeLeafIterator it = new BTreeLeafIterator(tree);
		
		while(it.hasNext()) {
			System.out.println(it.next().getKey());
		}
		
		it.next();
	}

    @Test
    public void testChangedTree() {
        int order = 4;
        UniquePagedBTree tree = new UniquePagedBTree(order, new BTreeMemoryBufferManager());

        ArrayList<LongLongIndex.LLEntry> entries = new ArrayList<>();
        int limit = 10;
        for (int i = 0; i < limit; i++) {
            long key = i * 2;
            long value = i * 2;
            entries.add(new LongLongIndex.LLEntry(key, value));
            tree.insert(key, value);
        }

        BTreeLeafIterator iterator = new BTreeLeafIterator(tree);
        for (int i = 0; i < limit; i++) {
            long key = i * 2 + 1;
            long value = i * 2 + 1;
            tree.insert(key, value);
        }
        int i = 0;
        while (iterator.hasNext()) {
            LongLongIndex.LLEntry returned = iterator.next();
            assertEquals(entries.get(i).getKey(), returned.getKey());
            assertEquals(entries.get(i).getValue(), returned.getValue());
            i++;
        }
    }

}
