package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory {
	
	public BTreeNode newUniqueNode(int order, boolean isLeaf, boolean isRoot);

    public BTreeNode newNonUniqueNode(int order, boolean isLeaf, boolean isRoot);
}
