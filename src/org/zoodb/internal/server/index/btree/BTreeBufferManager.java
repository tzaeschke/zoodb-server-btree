package org.zoodb.internal.server.index.btree;

public interface BTreeBufferManager {

	/*
	 * returns null if pageId can not be found
	 */
	public PagedBTreeNode read(int pageId);

	public int write(PagedBTreeNode node);

}
