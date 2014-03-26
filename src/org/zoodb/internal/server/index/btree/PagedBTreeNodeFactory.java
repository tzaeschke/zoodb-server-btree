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

	public static PagedBTreeNode constructLeaf(
			BTreeBufferManager bufferManager, boolean isRoot, int order,
			int pageId, int numKeys, long[] keys, long[] values) {
		PagedBTreeNode node = new PagedBTreeNode(bufferManager, order,
				true, isRoot, pageId);

		node.setNumKeys(numKeys);
		node.setKeys(keys);
		node.setValues(values);
		return node;
	}
	
    public static PagedBTreeNode constructInnerNode(
			BTreeBufferManager bufferManager, boolean isRoot, int order,
			int pageId, int numKeys, long[] keys, int[] childrenPageIds) {
		PagedBTreeNode node = new PagedBTreeNode(bufferManager, order,
				false, isRoot, pageId);

		node.setNumKeys(numKeys);
		node.setKeys(keys);
		node.setChildrenPageIds(childrenPageIds);
		return node;
	}

}
