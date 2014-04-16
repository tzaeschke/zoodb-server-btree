package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.AscendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.DescendingBTreeLeafEntryIterator;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;

/**
 * Abstracts the need to specify a BTreeNodeFactory, which is specific to this type of tree.
 *
 * Also, adds the buffer manager that will be used by this type of node as an argument.
 */
public class UniquePagedBTree extends UniqueBTree<PagedBTreeNode> {

    private BTreeBufferManager bufferManager;

    public UniquePagedBTree(int order, BTreeBufferManager bufferManager) {
        super(order, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    public UniquePagedBTree(int innerNodeOrder, int leafOrder, BTreeBufferManager bufferManager) {
        super(innerNodeOrder, leafOrder, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    @Override
    public long delete(long key) {
        //TODO need to all nodes involved in delete as dirty, not just the path down
        return super.delete(key);
    }

    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }
    
    public void write() {
    	bufferManager.write(getRoot());
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
