package org.zoodb.internal.server.index.btree;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.PrimLongMapZ;

public class BTreeStorageBufferManager implements BTreeBufferManager {

	private final int leafOrder;
	private final int innerNodeOrder;
	
	private PrimLongMapZ<PagedBTreeNode> dirtyBuffer;
	private PrimLongMapZ<PagedBTreeNode> cleanBuffer;
	private final int maxCleanBufferElements = -1;

	private int pageIdCounter;
	private final boolean isUnique;

	private final IOResourceProvider storageFile;
	private PAGE_TYPE dataType = PAGE_TYPE.GENERIC_INDEX;;
	
	private int statNWrittenPages = 0;
	private int statNReadPages = 0;

	public BTreeStorageBufferManager(IOResourceProvider storage, boolean isUnique) {
		this.dirtyBuffer = new PrimLongMapZ<>();
		this.cleanBuffer = new PrimLongMapZ<>();
		this.pageIdCounter = 0;
		this.isUnique = isUnique;
		this.storageFile = storage;
		
    	int pageSize = this.storageFile.getPageSize();
    	this.leafOrder = computeLeafOrder(pageSize);
    	this.innerNodeOrder = computeInnerNodeOrder(pageSize, isUnique);
	}

	public BTreeStorageBufferManager(IOResourceProvider storage, boolean isUnique, PAGE_TYPE dataType) {
		this(storage, isUnique);
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
		StorageChannelInput storageIn = getIO().getInputChannel();
		storageIn.seekPageForRead(dataType, pageId);

		PagedBTreeNode node;

		short isInner = storageIn.readShort();
		int numKeys;
		int order;

		if(isInner > -1) {
            // its an innner node
			numKeys = isInner;
			order = innerNodeOrder;
		} else { 
            // its a leaf
			numKeys = storageIn.readShort();
			order = leafOrder;
		}
		long[] keys = new long[order - 1];
		storageIn.noCheckRead(keys);
		//ToDo add values for unique
		if (isInner == -1) {
			// leaf
			long[] values = new long[order - 1];
			storageIn.noCheckRead(values);

			node = PagedBTreeNodeFactory.constructLeaf(this, isUnique, false,
										order, pageId, numKeys, 
										keys, values);
		} else {
			int[] childrenPageIds = new int[order];

			storageIn.noCheckRead(childrenPageIds);
            long[] values = null;
            if (!isUnique) {
                values = new long[order - 1];
                storageIn.noCheckRead(values);
            }
			node = PagedBTreeNodeFactory.constructInnerNode(this, isUnique, false,
								order, pageId, numKeys, keys, values,
								childrenPageIds);
		}
		getIO().returnInputChannel(storageIn);

		// node in memory == node in storage
		node.markClean();
		putInCleanBuffer(node.getPageId(), node);
		
		statNReadPages++;

		return node;
	}


	@Override
	public int write(PagedBTreeNode node, StorageChannelOutput out) {
		if (!node.isDirty()) {
			return node.getPageId();
		}
		if (!node.isLeaf()) /* is inner node */{
			// write children
			int childIndex = 0;
			for (int childPageId : node.getChildrenPageIdList()) {
				PagedBTreeNode child = readNodeFromMemory(childPageId);
                //if child is not in memory, then it can not be dirty
				if(child != null && child.isDirty()) {
                    int newChildPageId = write(child, out);
                    node.setChildPageId(childIndex, newChildPageId);
				}

				childIndex++;
			}
		}
		// write data to storage and obtain new pageId
		int newPageId = writeNodeDataToStorage(node, out);

		// update pageId in memory
		dirtyBuffer.remove(node.getPageId());
		putInCleanBuffer(newPageId, node);

		node.setPageId(newPageId);
		node.markClean();
		
		statNWrittenPages++;

		return newPageId;
	}

	private void putInCleanBuffer(int pageId, PagedBTreeNode node) {
		if(maxCleanBufferElements < 0 || cleanBuffer.size() < maxCleanBufferElements - 1) {
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

	int writeNodeDataToStorage(PagedBTreeNode node, StorageChannelOutput storageOut) {

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
            if (!isUnique) {
                storageOut.noCheckWrite(node.getValues());
            }
			storageOut.noCheckWrite(childrenPageIds);
		}

		//TODO flush??? Is that necessary??
		storageOut.flush();
		return pageId;
	}
	
    public static int computeLeafOrder(int pageSize) {
    	int headerSize = DiskIO.PAGE_HEADER_SIZE;
    	int leafIndicatorSize = 2;
    	int numKeysSize = 2;
    	
    	int keySize = 8;
    	int valueSize = 8;
    	
    	if(pageSize < headerSize + leafIndicatorSize + numKeysSize) {
			throw DBLogger.newFatal("Illegal Page size: " + pageSize);
    	}

    	int order = 0;
    	pageSize -= (headerSize + leafIndicatorSize + numKeysSize);
    	order = (int)Math.round(Math.floor(pageSize/(keySize + valueSize)));
    	
    	if(order < 2) {
			throw DBLogger.newFatal("Illegal Page size: " + pageSize);
    	}

		return order+1;
	}
    
    public static int computeInnerNodeOrder(int pageSize, boolean isUnique) {
    	int headerSize = DiskIO.PAGE_HEADER_SIZE;
    	int numKeysSize = 2;
    	
    	int keySize = isUnique ? 8 : 16;
    	int childrenSize = 4;
    	
    	if(pageSize < headerSize + numKeysSize) {
			throw DBLogger.newFatal("Illegal Page size: " + pageSize);
    	}

    	int order = 0;
    	pageSize -= (headerSize + numKeysSize);
    	
    	pageSize -= childrenSize;
    	order = (int)Math.round(Math.floor(pageSize/(keySize + childrenSize))) + 1;
    	
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
		for(long id : dirtyBuffer.keySet()) {
            if(id > 0) {
                // page has been written to storage
                this.storageFile.reportFreePage((int)id);
            }
		}
		dirtyBuffer.clear();
		
        for(long id : cleanBuffer.keySet()) {
            if(id > 0) {
                // page has been written to storage
                this.storageFile.reportFreePage((int)id);
            }
		}
		cleanBuffer.clear();
	}

	public PrimLongMapZ<PagedBTreeNode> getMemoryBuffer() {
        PrimLongMapZ<PagedBTreeNode> ret = new PrimLongMapZ<PagedBTreeNode>();
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
	
	public int getLeafOrder() {
		return leafOrder;
	}

	public int getInnerNodeOrder() {
		return innerNodeOrder;
	}

	public PrimLongMapZ<PagedBTreeNode> getDirtyBuffer() {
		return dirtyBuffer;
	}

	public PrimLongMapZ<PagedBTreeNode> getCleanBuffer() {
		return cleanBuffer;
	}
	
    public int getStatNWrittenPages() {
		return statNWrittenPages;
	}

	public int getStatNReadPages() {
		return statNReadPages;
	}
	
	/*
	 * Iterates through tree and returns pageId of every reachable node
	 * that has been written to storage. Every non-reachable node should have
	 * been removed using BTree.remove and thus its page has been reported
	 * as free.
	 */
	public <T extends PagedBTreeNode> List<Integer> debugPageIds(PagedBTree<T> tree) {
		BTreeIterator it = new BTreeIterator(tree);
		ArrayList<Integer> pageIds = new ArrayList<Integer>();
		while(it.hasNext()) {
			PagedBTreeNode node = (PagedBTreeNode) it.next();
			int pageId = node.getPageId();
			if(pageId > 0) {
				pageIds.add(pageId);
			}
		}
		return pageIds;
	}

	@Override
	public IOResourceProvider getIO() {
		return this.storageFile;
	}
}
