package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.AscendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;

public class NonUniquePagedBTree extends NonUniqueBTree<PagedBTreeNode> {

    private BTreeBufferManager bufferManager;

    public NonUniquePagedBTree(int order, BTreeBufferManager bufferManager) {
        super(order, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    public NonUniquePagedBTree(int innerNodeOrder, int leafOrder, BTreeBufferManager bufferManager) {
        super(innerNodeOrder, leafOrder, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }
    
    public long getMinKey() {
    	BTreeLeafEntryIterator<PagedBTreeNode> it = new AscendingBTreeLeafEntryIterator<PagedBTreeNode>(this);
    	long minKey = 0;
		if(it.hasNext()) {
			minKey = it.next().getKey();
		}
		return minKey;
    }
    
    public long getMaxKey() {
    	BTreeLeafEntryIterator<PagedBTreeNode> it = new DescendingBTreeLeafEntryIterator<PagedBTreeNode>(this);
    	long maxKey = 0;
		if(it.hasNext()) {
			maxKey = it.next().getKey();
		}
		return maxKey;
    }

}
