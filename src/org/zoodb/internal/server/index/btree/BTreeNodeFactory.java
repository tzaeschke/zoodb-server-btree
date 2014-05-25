package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory {
	
	public BTreeNode newUniqueNode(int pageSize, boolean isLeaf, boolean isRoot);

    public BTreeNode newNonUniqueNode(int pageSize, boolean isLeaf, boolean isRoot);

    public BTreeNode newNode(boolean isUnique, int pageSize, boolean isLeaf, boolean isRoot);

}
