package org.zoodb.internal.server.index.btree;


public interface BTreeNodeFactory {
	
	public <T extends BTreeNode> T newUniqueNode(int order, boolean isLeaf, boolean isRoot);

    public <T extends BTreeNode> T newNonUniqueNode(int order, boolean isLeaf, boolean isRoot);
}
