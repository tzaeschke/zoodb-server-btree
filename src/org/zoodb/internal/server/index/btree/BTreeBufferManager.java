package org.zoodb.internal.server.index.btree;

import java.util.Observer;

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
	public int write(PagedBTreeNode node);
	
    /*
	 * returns page size
	 */
	public int getPageSize();
	
	/*
	 * returns the size of the node in storage (including all metadata)
	 */
	public int getNodeSizeInStorage(PagedBTreeNode node);

    /*
     * return the size of the storage metadata for a node
     */
    public int getNodeHeaderSizeInStorage(PagedBTreeNode node);
    
    /*
     * returns the size in bytes of a nodes value element
     */
	public int getNodeValueElementSize();

    /*
	 * writes the node to the storage channel
	 */
	public void clear();




}
