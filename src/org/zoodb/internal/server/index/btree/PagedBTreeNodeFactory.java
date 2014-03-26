package org.zoodb.internal.server.index.btree;

public class PagedBTreeNodeFactory implements BTreeNodeFactory {

	private BTreeBufferManager bufferManager;

	public PagedBTreeNodeFactory(BTreeBufferManager bufferManager) {
		this.bufferManager = bufferManager;
	}

	@Override
	public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
		return new PagedBTreeNode(bufferManager, order, isLeaf, isRoot);
	}
}
