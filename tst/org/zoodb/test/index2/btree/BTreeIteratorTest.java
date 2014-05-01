package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.btree.AscendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.PagedBTree;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;

@RunWith(Parameterized.class)
public class BTreeIteratorTest {

	private PagedBTree<?> testTree;

	public BTreeIteratorTest(PagedBTree<?> testTree) {
		this.testTree = testTree;
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				{ new NonUniquePagedBTree(4, new BTreeMemoryBufferManager()) },
				{ new UniquePagedBTree(4, new BTreeMemoryBufferManager()) } });
	}

	@Test(expected = NoSuchElementException.class)
	public void testLeafIterate() {
		PagedBTree<?> tree = TestBTree
				.getTestTree(new BTreeMemoryBufferManager());

		BTreeLeafEntryIterator<?> it = new AscendingBTreeLeafEntryIterator(
				tree, 3, 15);

		while (it.hasNext()) {
			it.next().getKey();
		}

		it.next();
	}

	@Test
	public void testAscendingIterator() {
		int order = 4;
		BTree tree = new NonUniquePagedBTree(order,
				new BTreeMemoryBufferManager());

		ArrayList<LongLongIndex.LLEntry> entries = new ArrayList<>();
		int limit = 10000;
		for (int i = 0; i < limit; i++) {
			long key = i * 2;
			long value = i * 2;
			entries.add(new LongLongIndex.LLEntry(key, value));
			tree.insert(key, value);
		}

		BTreeLeafEntryIterator iterator = new AscendingBTreeLeafEntryIterator(
				tree);
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

		BTree tree = new NonUniquePagedBTree(order,
				new BTreeMemoryBufferManager());

		ArrayList<LongLongIndex.LLEntry> entries = new ArrayList<>();
		int limit = 10000;
		for (int i = 0; i < limit; i++) {
			long key = i * 2;
			long value = i * 2;
			entries.add(new LongLongIndex.LLEntry(key, value));
			tree.insert(key, value);
		}

		BTreeLeafEntryIterator iterator = new DescendingBTreeLeafEntryIterator(
				tree);
		int i = limit - 1;
		while (iterator.hasNext()) {
			LongLongIndex.LLEntry returned = iterator.next();
			assertEquals(entries.get(i).getKey(), returned.getKey());
			assertEquals(entries.get(i).getValue(), returned.getValue());
			i--;
		}

	}

	@Test
	public void testRangeIteratorUnique() {
		int order = 4;
		PagedBTree<?> tree = new UniquePagedBTree(order,
				new BTreeMemoryBufferManager());

		int limit = 4;
		for (int i = 0; i < limit; i++) {
			tree.insert(i, i + 32);
		}

		/*
		 * Ascending
		 */
		BTreeLeafEntryIterator<?> it = new AscendingBTreeLeafEntryIterator<>(
				tree, 0, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(32L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, 1, 1);
		assertEquals(new ArrayList<Long>(Arrays.asList(33L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, -1, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(32L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, 3, 4);
		assertEquals(new ArrayList<Long>(Arrays.asList(35L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, 0, 3);
		assertEquals(new ArrayList<Long>(Arrays.asList(32L, 33L, 34L, 35L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, -1, 4);
		assertEquals(new ArrayList<Long>(Arrays.asList(32L, 33L, 34L, 35L)),
				valueListFromIterator(it));
		

		/*
		 * Descending
		 */
		it = new DescendingBTreeLeafEntryIterator<>(tree, 0, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(32L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 1, 1);
		assertEquals(new ArrayList<Long>(Arrays.asList(33L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, -1, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(32L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 3, 3);
		assertEquals(new ArrayList<Long>(Arrays.asList(35L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 3, 4);
		assertEquals(new ArrayList<Long>(Arrays.asList(35L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 0, 3);
		assertEquals(new ArrayList<Long>(Arrays.asList(35L, 34L, 33L, 32L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, -1, 4);
		assertEquals(new ArrayList<Long>(Arrays.asList(35L, 34L, 33L, 32L)),
				valueListFromIterator(it));

	}

	@Test
	public void testRangeIteratorNonUnique() {
		int order = 4;
		PagedBTree<?> tree = new NonUniquePagedBTree(order,
				new BTreeMemoryBufferManager());

		tree.insert(0, 0);
		tree.insert(0, 1);
		tree.insert(2, 2);
		tree.insert(3, 3);
		tree.insert(3, 4);
		tree.insert(3, 5);
		
		/*
		 * Ascending
		 */
		BTreeLeafEntryIterator<?> it = new AscendingBTreeLeafEntryIterator<>(
				tree, 0, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(0L, 1L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, -1, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(0L, 1L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, 2, 2);
		assertEquals(new ArrayList<Long>(Arrays.asList(2L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, 3, 4);
		assertEquals(new ArrayList<Long>(Arrays.asList(3L, 4L, 5L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, 0, 3);
		assertEquals(
				new ArrayList<Long>(Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L)),
				valueListFromIterator(it));

		it = new AscendingBTreeLeafEntryIterator<>(tree, -1, 4);
		assertEquals(
				new ArrayList<Long>(Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L)),
				valueListFromIterator(it));

		/*
		 * Descending
		 */
		it = new DescendingBTreeLeafEntryIterator<>(tree, 0, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(1L, 0L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 2, 2);
		assertEquals(new ArrayList<Long>(Arrays.asList(2L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, -1, 0);
		assertEquals(new ArrayList<Long>(Arrays.asList(1L, 0L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 2, 3);
		assertEquals(new ArrayList<Long>(Arrays.asList(5L, 4L, 3L, 2L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 3, 4);
		assertEquals(new ArrayList<Long>(Arrays.asList(5L, 4L, 3L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, 0, 3);
		assertEquals(
				new ArrayList<Long>(Arrays.asList(5L, 4L, 3L, 2L, 1L, 0L)),
				valueListFromIterator(it));

		it = new DescendingBTreeLeafEntryIterator<>(tree, -1, 4);
		assertEquals(
				new ArrayList<Long>(Arrays.asList(5L, 4L, 3L, 2L, 1L, 0L)),
				valueListFromIterator(it));
	}
	
	@Test 
	public void outsideRangeTest() {
		int order = 4;
		PagedBTree<?> tree = new UniquePagedBTree(order,
				new BTreeMemoryBufferManager());

		int limit = 4;
		for (int i = 0; i < limit; i++) {
			tree.insert(i, i + 32);
		}
	    assertFalse(new AscendingBTreeLeafEntryIterator<>(tree, -1, -1).hasNext());
	    assertFalse(new AscendingBTreeLeafEntryIterator<>(tree, 4, 5).hasNext());
	    assertFalse(new DescendingBTreeLeafEntryIterator<>(tree, -1, -1).hasNext());
	    assertFalse(new DescendingBTreeLeafEntryIterator<>(tree, 4, 5).hasNext());
	    
		tree = new NonUniquePagedBTree(order,
				new BTreeMemoryBufferManager());

		tree.insert(0, 0);
		tree.insert(0, 1);
		tree.insert(2, 2);
		tree.insert(3, 3);
		tree.insert(3, 4);
		tree.insert(3, 5);
		
	    assertFalse(new AscendingBTreeLeafEntryIterator<>(tree, -1, -1).hasNext());
	    assertFalse(new AscendingBTreeLeafEntryIterator<>(tree, 4, 5).hasNext());
	    assertFalse(new DescendingBTreeLeafEntryIterator<>(tree, -1, -1).hasNext());
	    assertFalse(new DescendingBTreeLeafEntryIterator<>(tree, 4, 5).hasNext());
		
	}
	
    @Test 
	public void emptyTreeTest() {
		int order = 4;
		UniquePagedBTree tree = new UniquePagedBTree(order,
				new BTreeMemoryBufferManager());

        tree.insert(1, 2);
        tree.delete(1);
        
        BTreeLeafEntryIterator<?> it = new AscendingBTreeLeafEntryIterator<>(tree);
        assertFalse(it.hasNext());
        
        it = new DescendingBTreeLeafEntryIterator<>(tree);
        assertFalse(it.hasNext());
    }

	public ArrayList<Long> valueListFromIterator(BTreeLeafEntryIterator<?> it) {
		ArrayList<Long> values = new ArrayList<>();
		while (it.hasNext()) {
			values.add(it.next().getValue());
		}
		return values;
	}
}
