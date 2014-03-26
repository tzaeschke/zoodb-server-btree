package org.zoodb.internal.server.index.btree;


public class MemoryBTreeNodeFactory implements BTreeNodeFactory {

	public MemoryBTreeNodeFactory() {
	}

	@Override
	public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
        return new MemoryBTreeNode(order, isLeaf, isRoot);
	}

}
