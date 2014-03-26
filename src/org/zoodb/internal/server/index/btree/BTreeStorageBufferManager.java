package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;

import java.util.HashMap;
import java.util.Map;

public class BTreeStorageBufferManager implements BTreeBufferManager {

	private Map<Integer, PagedBTreeNode> map;
	private int pageIdCounter;
	private final StorageChannelInput storageIn;
	private final StorageChannelOutput storageOut;
	// TODO: is this the correct data type?
	private final DATA_TYPE dataType = DATA_TYPE.GENERIC_INDEX;

	public BTreeStorageBufferManager(StorageChannel storage) {
		this.map = new HashMap<Integer, PagedBTreeNode>();
		this.pageIdCounter = 0;
		this.storageIn = storage.getReader(false);
		this.storageOut = storage.getWriter(false);
	}

	@Override
	public PagedBTreeNode read(int pageId) {
		// search node in memory
		PagedBTreeNode node = map.get(pageId);
		if (node != null) {
			return node;
		}

		// search node in storage
		return readNodeFromStorage(pageId);
	}

	public PagedBTreeNode readNodeFromStorage(int pageId) {
		// TODO: the constructor of nodes is called with
		// parent = null!
		
		// TODO: do not use so many setters, make constructors/
		PagedBTreeNode node;

		short order = storageIn.readShort();
		if (order == 0) {
			// leaf
			node = new PagedBTreeNode(this, order, true, true, pageId);
			int numKeys = storageIn.readShort();
			long[] keys = new long[order - 1];
			long[] values = new long[order - 1];
			storageIn.noCheckRead(keys);
			storageIn.noCheckRead(values);

			node = PagedBTreeNodeFactory.constructLeaf(this, true,
										order, pageId, numKeys, 
										keys, values);
		} else {
			node = new PagedBTreeNode(this, order, false, true, pageId);
			int[] childrenPageIds = new int[order];
			long[] keys = new long[order - 1];

			storageIn.noCheckRead(childrenPageIds);
			int numKeys = storageIn.readShort();
			storageIn.noCheckRead(keys);
			
			node = PagedBTreeNodeFactory.constructInnerNode(this, true,
								order, pageId, numKeys, keys, 
								childrenPageIds);
		}

		// node in memory == node in storage
		node.markClean();
		return node;
	}

	@Override
	public int write(PagedBTreeNode node) {
		if (!node.isDirty()) {
			return node.getPageId();
		}
		if (!node.isLeaf()) /* is inner node */{
			// write children
			int childIndex = 0;
			for (int childPageId : node.getChildrenPageIdList()) {
				// TODO: optimize: if children is not in memory, then it can not
				// be dirty
				PagedBTreeNode child = read(childPageId);
				int newChildPageId = write(child);
				node.setChildPageId(childIndex, childPageId);

				childIndex++;
			}
		}
		// write data to storage and obtain new pageId
		int newPageId = writeNodeDataToStorage(node);

		// update pageId in memory
		map.remove(node.getPageId());
		// TODO: handle case when newPageId is already in map for another node
		// n'.
		// This could happen if n' was never written to the storage before, so
		// it received a pageId which is not associated with a page.
		map.put(newPageId, node);

		node.setPageId(newPageId);

		node.markClean();
		return newPageId;
	}

	/*
	 * Leaf node page: 
	 * 2 byte 0 
	 * 2 byte numKeys 
	 * 8 byte * order-1 keys 
	 * 8 byte * order-1 values
	 * 
	 * Inner node page 
	 * 2 byte order 
	 * 8 byte * order childrenPageIds 
	 * 2 byte numKeys 
	 * 8 byte * order-1 keys
	 */

	int writeNodeDataToStorage(PagedBTreeNode node) {
		// TODO: reasonable previous page id
		int pageId = storageOut.allocateAndSeek(dataType, 0);

		if (node.isLeaf()) {
			storageOut.writeShort((short) 0);
			storageOut.noCheckWrite(node.getKeys());

		} else {
			int[] childrenPageIds = node.getChildrenPageIds();
			storageOut.writeShort((short) node.getOrder());
			storageOut.noCheckWrite(childrenPageIds);
			storageOut.writeShort((short) node.getNumKeys());
			storageOut.noCheckWrite(node.getKeys());
		}

		storageOut.flush();
		return pageId;
	}

	@Override
	public int save(PagedBTreeNode node) {
		pageIdCounter++;
		// TODO: handle case that this key is already reserved for the "real" id
		// of a page.
		map.put(pageIdCounter, node);

		return pageIdCounter;
	}

	@Override
	public void delete(int id) {
		map.remove(id);
		// TODO: call delete
		// TODO: free page in storage

	}

	@Override
	public void clear() {
		pageIdCounter = 0;
		map.clear();
		// TODO clear storage
	}
}
