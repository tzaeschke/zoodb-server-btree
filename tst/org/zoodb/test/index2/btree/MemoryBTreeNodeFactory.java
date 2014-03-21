package org.zoodb.test.index2.btree;

import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.MemoryBTreeNode;

public class MemoryBTreeNodeFactory implements BTreeNodeFactory {

	public MemoryBTreeNodeFactory() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf) {
		return new MemoryBTreeNode(parent, order, isLeaf);
	}

}
