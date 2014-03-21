package org.zoodb.internal.server.index.btree;

public class PagedBTreeNodeFactory implements BTreeNodeFactory {

	private BTreeBufferManager bufferManager;

	public PagedBTreeNodeFactory(BTreeBufferManager bufferManager) {
		this.bufferManager = bufferManager;
	}

	@Override
	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf) {
		return new PagedBTreeNode(bufferManager, parent, order, isLeaf);
	}
}
