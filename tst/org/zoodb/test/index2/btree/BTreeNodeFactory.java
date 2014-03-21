package org.zoodb.test.index2.btree;

import org.zoodb.internal.server.index.btree.BTreeNode;

public interface BTreeNodeFactory {
	
	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf);

}
