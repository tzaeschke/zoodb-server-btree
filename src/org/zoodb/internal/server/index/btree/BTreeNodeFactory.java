package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory {
	
	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf);

}
