package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
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
}
