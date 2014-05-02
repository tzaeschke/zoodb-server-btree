package org.zoodb.internal.server.index.btree;

public abstract class PagedBTree<T extends PagedBTreeNode> extends BTree<T> {

    private BTreeBufferManager bufferManager;

	public PagedBTree(int pageSize, BTreeBufferManager bufferManager) {
		super(pageSize, new PagedBTreeNodeFactory(bufferManager));
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
