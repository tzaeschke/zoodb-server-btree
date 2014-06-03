package org.zoodb.internal.server.index.btree;

public abstract class PagedBTree extends BTree {

    private BTreeBufferManager bufferManager;

	public PagedBTree(PagedBTreeNode root, int pageSize,
			BTreeBufferManager bufferManager, boolean isUnique) {
		super(root, pageSize, new PagedBTreeNodeFactory(bufferManager), isUnique);
        this.bufferManager = bufferManager;
	}
	
	public PagedBTree(int pageSize, BTreeBufferManager bufferManager, boolean isUnique) {
		super(pageSize, new PagedBTreeNodeFactory(bufferManager), isUnique);
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
