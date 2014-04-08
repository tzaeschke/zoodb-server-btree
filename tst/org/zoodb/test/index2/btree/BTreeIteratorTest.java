package org.zoodb.test.index2.btree;

import java.util.NoSuchElementException;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeLeafIterator;
import org.zoodb.internal.server.index.btree.BTreeMemoryBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;

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

}
