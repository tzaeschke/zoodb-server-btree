package org.zoodb.internal.server.index.btree;


public class MemoryBTreeNodeFactory implements BTreeNodeFactory {

	public MemoryBTreeNodeFactory() {
	}

	@Override
	public BTreeNode newUniqueNode(int order, boolean isLeaf, boolean isRoot) {
        return new MemoryBTreeNode(order, isLeaf, isRoot);
	}

    @Override
    public BTreeNode newNonUniqueNode(int order, boolean isLeaf, boolean isRoot) {
        //TODO clean this up
        return null;
    }

}
