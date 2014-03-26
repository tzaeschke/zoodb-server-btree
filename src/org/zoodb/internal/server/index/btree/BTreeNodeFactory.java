package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory {
	
	public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot);

}
