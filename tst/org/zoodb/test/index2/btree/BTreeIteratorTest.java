package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.btree.*;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BTreeIteratorTest {

    private PagedBTree<?> testTree;

    public BTreeIteratorTest(PagedBTree<?> testTree) {
        this.testTree = testTree;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList( new Object[][]{
                { new NonUniquePagedBTree(4, new BTreeMemoryBufferManager()) },
                { new UniquePagedBTree(4, new BTreeMemoryBufferManager()) }
        }
        );
    }

    @Test(expected = NoSuchElementException.class)
	public void testLeafIterate() {
		PagedBTree<?> tree = TestBTree.getTestTree(new BTreeMemoryBufferManager());
		System.out.println(tree);
		
		BTreeLeafEntryIterator<?> it = new AscendingBTreeLeafEntryIterator(tree, 3, 15);
		
		while(it.hasNext()) {
			System.out.println(it.next().getKey());
		}
		
		it.next();
	}

    @Test
    public void testAscendingIterator() {
        int order = 4;
        BTree tree = new NonUniquePagedBTree(order, new BTreeMemoryBufferManager());

        ArrayList<LongLongIndex.LLEntry> entries = new ArrayList<>();
        int limit = 10000;
        for (int i = 0; i < limit; i++) {
            long key = i * 2;
            long value = i * 2;
            entries.add(new LongLongIndex.LLEntry(key, value));
            tree.insert(key, value);
        }

        BTreeLeafEntryIterator iterator = new AscendingBTreeLeafEntryIterator(tree);
        int i = 0;
        while (iterator.hasNext()) {
            LongLongIndex.LLEntry returned = iterator.next();
            assertEquals(entries.get(i).getKey(), returned.getKey());
            assertEquals(entries.get(i).getValue(), returned.getValue());
            i++;
        }
    }

    @Test
    public void testDescendingIterator() {
        int order = 4;

        BTree tree = new NonUniquePagedBTree(order, new BTreeMemoryBufferManager());

        ArrayList<LongLongIndex.LLEntry> entries = new ArrayList<>();
        int limit = 10000;
        for (int i = 0; i < limit; i++) {
            long key = i * 2;
            long value = i * 2;
            entries.add(new LongLongIndex.LLEntry(key, value));
            tree.insert(key, value);
        }

        BTreeLeafEntryIterator iterator = new DescendingBTreeLeafEntryIterator(tree);
        int i = limit - 1;
        while (iterator.hasNext()) {
            LongLongIndex.LLEntry returned = iterator.next();
            assertEquals(entries.get(i).getKey(), returned.getKey());
            assertEquals(entries.get(i).getValue(), returned.getValue());
            i--;
        }

    }

}