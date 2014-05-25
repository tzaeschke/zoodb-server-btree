package org.zoodb.internal.server.index.btree;

public abstract class PagedBTree extends BTree {

    private BTreeBufferManager bufferManager;

	public PagedBTree(PagedBTreeNode root, int pageSize,
			BTreeBufferManager bufferManager) {
		super(root, pageSize, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
	}
	
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
    
    public PagedBTreeNode getRoot() {
    	return (PagedBTreeNode) root;
    }
}
