package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory<T extends BTreeNode> {
	
	public T newUniqueNode(int order, boolean isLeaf, boolean isRoot);

    public T newNonUniqueNode(int order, boolean isLeaf, boolean isRoot);

    public T newNode(boolean isUnique, int order, boolean isLeaf, boolean isRoot);

}
