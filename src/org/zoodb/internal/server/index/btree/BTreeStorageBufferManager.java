package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;

import java.util.HashMap;
import java.util.Map;
import java.util.Observable;

public class BTreeStorageBufferManager implements BTreeBufferManager {

	private Map<Integer, PagedBTreeNode> dirtyBuffer;
	private Map<Integer, PagedBTreeNode> cleanBuffer;
	private final int maxCleanBufferElements = 2000;

	private int pageIdCounter;

	private final StorageChannel storageFile;
	private final StorageChannelInput storageIn;
	private final StorageChannelOutput storageOut;
	private DATA_TYPE dataType;
	

	public BTreeStorageBufferManager(StorageChannel storage) {
		this.dirtyBuffer = new HashMap<Integer, PagedBTreeNode>();
		this.cleanBuffer = new HashMap<Integer, PagedBTreeNode>();
		this.pageIdCounter = 0;
		this.storageFile = storage;
		this.storageIn = storage.getReader(false);
		this.storageOut = storage.getWriter(false);
        this.dataType = DATA_TYPE.GENERIC_INDEX;
	}

	public BTreeStorageBufferManager(StorageChannel storage, DATA_TYPE dataType) {
		this(storage);
		this.dataType = dataType;
	}

	/*
	 * Only read pageIds that are known to be in BufferManager,
	 * otherwise the result is undefined
	 */
	@Override
	public PagedBTreeNode read(int pageId) {
		// search node in memory
		PagedBTreeNode node = readNodeFromMemory(pageId);
		if (node != null) {
			return node;
		}

		// search node in storage
		return readNodeFromStorage(pageId);
	}

	public PagedBTreeNode readNodeFromMemory(int pageId) {
		PagedBTreeNode node = dirtyBuffer.get(pageId);
		if(node != null) {
			return node;
		}

		return cleanBuffer.get(pageId);
	}

	public PagedBTreeNode readNodeFromStorage(int pageId) {
        storageIn.seekPageForRead(dataType, pageId);

		PagedBTreeNode node;

		short orderIfInner = storageIn.readShort();
		if (orderIfInner == 0) {
			// leaf
			short order = storageIn.readShort();
			int numKeys = storageIn.readShort();
			long[] keys = new long[order - 1];
			long[] values = new long[order - 1];
			storageIn.noCheckRead(keys);
			storageIn.noCheckRead(values);

			node = PagedBTreeNodeFactory.constructLeaf(this, true,
										order, pageId, numKeys, 
										keys, values);
		} else {
			int[] childrenPageIds = new int[orderIfInner];
			long[] keys = new long[orderIfInner - 1];

			storageIn.noCheckRead(childrenPageIds);
			int numKeys = storageIn.readShort();
			storageIn.noCheckRead(keys);
			
			node = PagedBTreeNodeFactory.constructInnerNode(this, true,
								orderIfInner, pageId, numKeys, keys, 
								childrenPageIds);
		}

		// node in memory == node in storage
		node.markClean();
		putInCleanBuffer(node.getPageId(), node);

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
				PagedBTreeNode child = readNodeFromMemory(childPageId);
                //if child is not in memory, then it can not be dirty
				if(node != null && child.isDirty()) {
                    int newChildPageId = write(child);
                    node.setChildPageId(childIndex, newChildPageId);
				}

				childIndex++;
			}
		}
		// write data to storage and obtain new pageId
		int newPageId = writeNodeDataToStorage(node);

		// update pageId in memory
		dirtyBuffer.remove(node.getPageId());
		putInCleanBuffer(newPageId, node);

		node.setPageId(newPageId);

		node.markClean();
		return newPageId;
	}

	private void putInCleanBuffer(int pageId, PagedBTreeNode node) {
		if(cleanBuffer.size() < maxCleanBufferElements - 1) {
			cleanBuffer.put(pageId, node);
		} else {
			cleanBuffer.clear();
		}
	}

	/*
	 * Leaf node page: 
	 * 2 byte 0 
	 * 2 byte order
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
	// TODO: rework file format

	int writeNodeDataToStorage(PagedBTreeNode node) {

		int previousPageId = node.getPageId() < 0 ? 0 : node.getPageId();
		// if node was not written before (negative "page id") use 0
		// as previous page id
		int pageId = storageOut.allocateAndSeek(dataType, previousPageId);

		if (node.isLeaf()) {
			storageOut.writeShort((short) 0);
			storageOut.writeShort((short) node.getOrder());
			storageOut.writeShort((short) node.getNumKeys());
			storageOut.noCheckWrite(node.getKeys());
			storageOut.noCheckWrite(node.getValues());

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
		/*
		 * nodes which only reside in memory have a negative
		 * "page id".
		 */
		pageIdCounter--;
		if(node.isDirty()) {
            dirtyBuffer.put(pageIdCounter, node);
		} else {
            putInCleanBuffer(pageIdCounter, node);
		}

		return pageIdCounter;
	}

	@Override
	public void remove(int id) {
		if(dirtyBuffer.remove(id) == null) {
			cleanBuffer.remove(id);
		}
		if(id > 0) {
			// page has been written to storage
			this.storageFile.reportFreePage(id);
		}
	}

	@Override
	public void clear() {
		pageIdCounter = 0;
		for(int id : dirtyBuffer.keySet()) {
            if(id > 0) {
                // page has been written to storage
                this.storageFile.reportFreePage(id);
            }
		}
		dirtyBuffer.clear();
		
        for(int id : cleanBuffer.keySet()) {
            if(id > 0) {
                // page has been written to storage
                this.storageFile.reportFreePage(id);
            }
		}
		cleanBuffer.clear();
	}

	public Map<Integer, PagedBTreeNode> getMemoryBuffer() {
        Map<Integer, PagedBTreeNode> ret = new HashMap<Integer, PagedBTreeNode>();
        ret.putAll(cleanBuffer);
        ret.putAll(dirtyBuffer);

		return ret;
	}

	@Override
	public void update(Observable o, Object arg) {
		PagedBTreeNode node = (PagedBTreeNode) o;
		int pageId = node.getPageId();
		if(node.isDirty()) {
			cleanBuffer.remove(pageId);
			dirtyBuffer.put(pageId, node);
		} else {
			dirtyBuffer.remove(pageId);
			putInCleanBuffer(pageId, node);
		}
	}

	public Map<Integer, PagedBTreeNode> getDirtyBuffer() {
		return dirtyBuffer;
	}

	public Map<Integer, PagedBTreeNode> getCleanBuffer() {
		return cleanBuffer;
	}
}
