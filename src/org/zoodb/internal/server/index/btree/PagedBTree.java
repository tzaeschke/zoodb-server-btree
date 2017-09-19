package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.StorageChannelOutput;

public abstract class PagedBTree<T extends PagedBTreeNode> extends BTree<T> {

    private BTreeBufferManager bufferManager;

	public PagedBTree(T root, int innerNodeOrder, int leafOrder,
			BTreeBufferManager bufferManager) {
		super(root, innerNodeOrder, leafOrder, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
	}
	
	public PagedBTree(int innerNodeOrder, int leafOrder,
			BTreeBufferManager bufferManager) {
		super(innerNodeOrder, leafOrder, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
	}

	public PagedBTree(int order, BTreeBufferManager bufferManager) {
		super(order, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
	}
	
    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }
    
    public void write(StorageChannelOutput out) {
    	bufferManager.write(getRoot(), out);
    }
    
    @Override
    public T getRoot() {
    	return root;
    }

}
