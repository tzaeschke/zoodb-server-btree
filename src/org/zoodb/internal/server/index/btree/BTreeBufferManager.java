package org.zoodb.internal.server.index.btree;

import java.util.Observer;

import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelOutput;

public interface BTreeBufferManager extends Observer {

	/*
	 * returns null if pageId can not be found
	 */
	public PagedBTreeNode read(int pageId);

	/*
	 * saves the node in the buffer manager
	 */
	public int save(PagedBTreeNode node);
	
    /*
	 * deletes a node from the buffer manager
	 */
	public void remove(int pageId);
	
    /*
	 * writes the node to the storage channel
	 */
	public int write(PagedBTreeNode node, StorageChannelOutput out);
	
    /*
	 * writes the node to the storage channel
	 */
	public void clear();

	public IOResourceProvider getIO();

}
