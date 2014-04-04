package org.zoodb.internal.server.index.btree;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.test.index2.btree.BTreeFactory;

public class BTreeStorageBufferManager implements BTreeBufferManager {

	private Map<Integer, PagedBTreeNode> dirtyBuffer;
	private Map<Integer, PagedBTreeNode> cleanBuffer;
	private final int maxCleanBufferElements = 2000;

	private int pageIdCounter;

	private final StorageChannel storageFile;
	private final StorageChannelInput storageIn;
	private final StorageChannelOutput storageOut;
	private DATA_TYPE dataType;
	
	
	private int statNWrittenPages = 0;
	private int statNReadPages = 0;

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
		int order = computeOrder();

		short isInner = storageIn.readShort();
		int numKeys;
		if(isInner > -1) {
			numKeys = isInner;
		} else { 
			numKeys = storageIn.readShort();
		}
		long[] keys = new long[order - 1];
		storageIn.noCheckRead(keys);
		
        //ToDo save a boolean to distinguish between unique and non-unique nodes
        boolean isUnique = true;
		
		if (isInner == -1) {
			// leaf
			long[] values = new long[order - 1];
			storageIn.noCheckRead(values);

			node = PagedBTreeNodeFactory.constructLeaf(this, isUnique, true,
										order, pageId, numKeys, 
										keys, values);
		} else {
			int[] childrenPageIds = new int[order];

			storageIn.noCheckRead(childrenPageIds);
			
			node = PagedBTreeNodeFactory.constructInnerNode(this, isUnique, true,
								order, pageId, numKeys, keys, 
								childrenPageIds);
		}

		// node in memory == node in storage
		node.markClean();
		putInCleanBuffer(node.getPageId(), node);
		
		statNReadPages++;

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
		
		statNWrittenPages++;

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
	 * 2 byte -1 
	 * 2 byte numKeys 
	 * 8 byte * order-1 keys 
	 * 8 byte * order-1 values
	 * 
	 * Inner node page 
	 * 2 byte numKeys 
	 * 8 byte * order-1 keys
	 * 4 byte * order childrenPageIds 
	 */

	int writeNodeDataToStorage(PagedBTreeNode node) {

		int previousPageId = node.getPageId() < 0 ? 0 : node.getPageId();
		// if node was not written before (negative "page id") use 0
		// as previous page id
		int pageId = storageOut.allocateAndSeek(dataType, previousPageId);

		if (node.isLeaf()) {
			storageOut.writeShort((short) -1);
			storageOut.writeShort((short) node.getNumKeys());
			storageOut.noCheckWrite(node.getKeys());
			storageOut.noCheckWrite(node.getValues());

		} else {
			int[] childrenPageIds = node.getChildrenPageIds();
			storageOut.writeShort((short) node.getNumKeys());
			storageOut.noCheckWrite(node.getKeys());
			storageOut.noCheckWrite(childrenPageIds);
		}

		storageOut.flush();
		return pageId;
	}
	
    public int computeOrder() {
    	int pageSize = this.storageFile.getPageSize();
    	
    	if(pageSize < 4+12) {
			throw DBLogger.newFatal("Illegal Page size: " + pageSize);
    	}

    	// compute for leafs because they take more space
    	int order = 0;
    	// store leafIndicator, order and numKeys
    	pageSize -= (6 + DiskIO.PAGE_HEADER_SIZE);
    	// how many key-value-pairs fit in
    	order = (int)Math.round(Math.floor(pageSize/16.0));
    	
    	if(order < 2) {
			throw DBLogger.newFatal("Illegal Page size: " + pageSize);
    	}

		return order;
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
	
    public int getStatNWrittenPages() {
		return statNWrittenPages;
	}

	public int getStatNReadPages() {
		return statNReadPages;
	}
	
	public static BTree<PagedBTreeNode> getTestTree(BTreeStorageBufferManager bufferManager) {
		int order = bufferManager.computeOrder();
		BTreeFactory factory = new BTreeFactory(order, bufferManager, true);
		factory.addInnerLayer(Arrays.asList(Arrays.asList(17L)));
		factory.addInnerLayer(Arrays.asList(Arrays.asList(5L, 13L),
				Arrays.asList(24L, 30L)));
		factory.addLeafLayerDefault(Arrays.asList(Arrays.asList(2L, 3L),
				Arrays.asList(5L, 7L, 8L), Arrays.asList(14L, 16L),
				Arrays.asList(19L, 20L, 22L), Arrays.asList(24L, 27L, 29L),
				Arrays.asList(33L, 34L, 38L, 39L)));
		UniquePagedBTree tree = (UniquePagedBTree) factory.getTree();
		return tree;
	}


}
