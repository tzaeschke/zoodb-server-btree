package org.zoodb.internal.server.index.btree;

public abstract class PagedBTree<T extends PagedBTreeNode> extends BTree<T> {

    private BTreeBufferManager bufferManager;

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
    
    public void write() {
    	bufferManager.write(getRoot());
    }
    
    public T getRoot() {
    	return (T) root;
    }

}
