package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory<T extends BTreeNode> {
	
	public T newUniqueNode(int pageSize, boolean isLeaf, boolean isRoot);

    public T newNonUniqueNode(int pageSize, boolean isLeaf, boolean isRoot);

    public T newNode(boolean isUnique, int pageSize, boolean isLeaf, boolean isRoot);

}
