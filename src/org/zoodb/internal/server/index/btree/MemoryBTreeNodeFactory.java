package org.zoodb.internal.server.index.btree;


public class MemoryBTreeNodeFactory implements BTreeNodeFactory {

	public MemoryBTreeNodeFactory() {
	}

	@Override
	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf) {
		return new MemoryBTreeNode(parent, order, isLeaf);
	}

}
