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

    private BTree testTree;

    public BTreeIteratorTest(BTree testTree) {
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
		BTree<PagedBTreeNode> tree = TestBTree.getTestTree(new BTreeMemoryBufferManager());
		System.out.println(tree);
		
		BTreeLeafIterator it = new AscendingBTreeLeafIterator(tree);
		
		while(it.hasNext()) {
			System.out.println(it.next().getKey());
		}
		
		it.next();
	}

    @Test
    public void testChangedTree() {
        int order = 4;
        BTree tree = new NonUniquePagedBTree(order, new BTreeMemoryBufferManager());

        ArrayList<LongLongIndex.LLEntry> entries = new ArrayList<>();
        int limit = 10;
        for (int i = 0; i < limit; i++) {
            long key = i * 2;
            long value = i * 2;
            entries.add(new LongLongIndex.LLEntry(key, value));
            tree.insert(key, value);
        }

        BTreeLeafIterator iterator = new AscendingBTreeLeafIterator(tree);
        int i = 0;
        while (iterator.hasNext()) {
            LongLongIndex.LLEntry returned = iterator.next();
            assertEquals(entries.get(i).getKey(), returned.getKey());
            assertEquals(entries.get(i).getValue(), returned.getValue());
            i++;
        }
    }

}
