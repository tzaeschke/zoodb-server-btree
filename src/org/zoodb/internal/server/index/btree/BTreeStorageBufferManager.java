/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 *
 * This file is part of ZooDB.
 *
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 *
 * See the README and COPYING files for further information.
 */
package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.DiskIO;
import org.zoodb.internal.server.DiskIO.DATA_TYPE;
import org.zoodb.internal.server.StorageChannel;
import org.zoodb.internal.server.StorageChannelInput;
import org.zoodb.internal.server.StorageChannelOutput;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;
import org.zoodb.internal.util.DBLogger;
import org.zoodb.internal.util.PrimLongMapLI;

import java.util.ArrayList;
import java.util.List;

/**
 * Buffer Manager for the B+ tree using the database storage.
 * Fetches pages from disk and writes them if they are dirty.
 * Only supports storing *one* tree.
 *
 * - Supports caching through the dirty and clean buffers.
 * - Performs encoding of the key array before page write
 * - Performs decoding of the key array after page read
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public class BTreeStorageBufferManager implements BTreeBufferManager {

    private int pageSize;
    
    // stores dirty nodes
	private final PrimLongMapLI<PagedBTreeNode> dirtyBuffer;
	// stores clean nodes
	private final PrimLongMapLI<PagedBTreeNode> cleanBuffer;
	private int maxCleanBufferElements = -1;

	// counter to give nodes that are not written yet
	// a unique but non-existent "pageId". The counter
	// only decreases to distinguish proper pageIds from
	// these negative ids.
	private int pageIdCounter = 0;
	private final boolean isUnique;

	private final StorageChannel storageFile;
	private final StorageChannelInput storageIn;
	private final StorageChannelOutput storageOut;
	private DATA_TYPE dataType = DATA_TYPE.GENERIC_INDEX;;
	
	private int statNWrittenPages = 0;
	private int statNReadPages = 0;

	// size of a leafs value in byte
	private int nodeValueElementSize = 8;

	public BTreeStorageBufferManager(StorageChannel storage, boolean isUnique) {
		this.dirtyBuffer = new PrimLongMapLI<>();
		this.cleanBuffer = new PrimLongMapLI<>();
		this.isUnique = isUnique;
		this.storageFile = storage;
		this.storageIn = storage.getReader(false);
		this.storageOut = storage.getWriter(false);
    	this.pageSize = this.storageFile.getPageSize();
	}

	public BTreeStorageBufferManager(StorageChannel storage, boolean isUnique, DATA_TYPE dataType) {
		this(storage, isUnique);
		this.dataType = dataType;
	}

	/**
	 * Call this to get nodes from the buffer manager.
	 * It will first try to find the node in memory, if it 
	 * can not be found it reads the page from the storage 
	 * channel. 
	 * Only read pageIds that are known to be in the 
	 * BufferManager's cache or on storage,
	 * otherwise the result is undefined.
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

		boolean isLeaf = storageIn.readByte() < 0 ? true : false;
		
		/* Deal with prefix-sharing encoded keys */
		byte[] metadata = new byte[5];
		storageIn.noCheckRead(metadata);
		int numKeys = PrefixSharingHelper.byteArrayToInt(metadata, 0);
		byte prefixLength = metadata[4];
		int encodedArraySize = PrefixSharingHelper.encodedArraySizeWithoutMetadata(numKeys, prefixLength);
		byte[] encodedArrayWithoutMetadata = new byte[encodedArraySize];
		storageIn.noCheckRead(encodedArrayWithoutMetadata);
        int maxNumKeys = PagedBTreeNode.computeMaxPossibleEntries(isUnique, isLeaf, getPageSize(), nodeValueElementSize);

		long[] keys = PrefixSharingHelper.decodeArray(encodedArrayWithoutMetadata, numKeys, maxNumKeys, prefixLength);

		if(isLeaf) {
			long[] values = new long[maxNumKeys];
			readValues(values, numKeys);
			node = PagedBTreeNodeFactory.constructLeaf(this, isUnique, false,
								pageSize, pageId, numKeys,
								keys, values);
		} else {
			int[] childrenPageIds = new int[maxNumKeys+1];

            long[] values = null;
            if (!isUnique) {
                values = new long[maxNumKeys];
                readValues(values, numKeys);
            }
			storageIn.noCheckRead(childrenPageIds, numKeys+1);
			node = PagedBTreeNodeFactory.constructInnerNode(this, isUnique, false,
								pageSize, pageId, numKeys, keys, values,
								childrenPageIds);
		}

		// node in memory == node in storage
		node.markClean();
		putInCleanBuffer(node.getPageId(), node);
		
		statNReadPages++;

		return node;
	}
	
	private void readValues(long[] values, int numValues) {
		if(nodeValueElementSize == 8) {
			storageIn.noCheckRead(values, numValues);
		} else {
			for(int i = 0; i < numValues; i++) {
				if(nodeValueElementSize == 1) {
					values[i] = storageIn.readByte();
				}
				else if(nodeValueElementSize == 2) {
					values[i] = storageIn.readShort();
				}
				else if(nodeValueElementSize == 4) {
					values[i] = storageIn.readInt();
				} else {
					throw new UnsupportedOperationException();
				}
			}
		}
	}


	/**
	 * Recursively writes the tree starting at that node
	 * to the storage channel. If a node is not dirty
	 * the sub-tree will not be written.
	 */
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
				if(child != null && child.isDirty()) {
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

	/**
	 * Put a node in the clean buffer and handle the
	 * caching policy. For example clear the cache if the
	 * cache size is exceeded
	 * 
	 * @param pageId
	 * @param node
	 */
	private void putInCleanBuffer(int pageId, PagedBTreeNode node) {
		if(maxCleanBufferElements < 0 || cleanBuffer.size() < maxCleanBufferElements) {
			cleanBuffer.put(pageId, node);
		} else {
			DBLogger.warning("Flushing whole buffer.");
			//TODO TZ We should only flush old and rarely used elements. 
			cleanBuffer.clear();
		}
	}

	/**
	 * The following encoding is used to write a node to
	 * the storage channel: 
	 * 
	 * Leaf node page: 
	 * 1 byte -1 
	 * prefixShareEncoding(keys) 
	 * size(value) bytes * numKeys for values
	 * 
	 * Inner node page: 
	 * 1 byte 0 
	 * prefixShareEncoding(keys) 
	 * size(value) bytes * numKeys for values (if NonUniqueNode
	 * 4 byte * (numKeys + 1) for childrenPageIds 
	 */
	int writeNodeDataToStorage(PagedBTreeNode node) {

		int previousPageId = node.getPageId() < 0 ? 0 : node.getPageId();
		// if node was not written before (negative "page id") use 0
		// as previous page id
		int pageId = storageOut.allocateAndSeek(dataType, previousPageId);

		if (node.isLeaf()) {
			storageOut.writeByte((byte) -1);
			byte[] encodedKeys = PrefixSharingHelper.encodeArray(node.getKeys(), node.getNumKeys(), node.getPrefix());
			storageOut.noCheckWrite(encodedKeys);
			writeValues(node.getValues(), node.getNumKeys());

		} else {
			storageOut.writeByte((byte) 1);
			byte[] encodedKeys = PrefixSharingHelper.encodeArray(node.getKeys(), node.getNumKeys(), node.getPrefix());
			storageOut.noCheckWrite(encodedKeys);
            if (node.getValues() != null) {
				writeValues(node.getValues(), node.getNumKeys());
            }
			storageOut.noCheckWrite(node.getChildrenPageIds(), node.getNumKeys()+1);
		}

		storageOut.flush();
		return pageId;
	}
	
	private void writeValues(long[] values, int numValues) {
		if(nodeValueElementSize == 8) {
			storageOut.noCheckWrite(values, numValues);
		} else {
			for(int i = 0; i < numValues; i++) {
				if(nodeValueElementSize == 1) {
					storageOut.writeByte((byte)values[i]);
				}
				else if(nodeValueElementSize == 2) {
					storageOut.writeShort((short)values[i]);
				}
				else if(nodeValueElementSize == 4) {
					storageOut.writeInt((int)values[i]);
				} else {
					throw new UnsupportedOperationException();
				}
			}
		}
	}

	/**
	 * Saves a node in the buffer manager.
	 * Note that it does not write the node to storage but
	 * only stores it in memory.
	 */
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

	/**
	 * Removes a node from buffer manager and frees the page
	 * if it has been written before.
	 */
	@Override
	public void remove(PagedBTreeNode node) {
		int pageId = node.getPageId();
		if(node.isDirty()) {
			dirtyBuffer.remove(pageId);
		} else {
			cleanBuffer.remove(pageId);
		}
		if(pageId > 0) {
			// page has been written to storage
			this.storageFile.reportFreePage(pageId);
		}
	}
	
	/**
	 * Clears memory and recursively frees the pages of the 
	 * nodes.
	 */
	@Override
	public void clear(PagedBTreeNode root) {
		clearHelper(root);
		cleanBuffer.clear();
		dirtyBuffer.clear();
	}
	
	public void clearHelper(PagedBTreeNode node) {
		if(!node.isLeaf()) {
			for (int childPageId : node.getChildrenPageIdList()) {
				PagedBTreeNode child = read(childPageId);
				clearHelper(child);
			}
		}
		if(node.getPageId() > 0) {
            // page has been written to storage
			this.storageFile.reportFreePage(node.getPageId());
		}
	}
	
	public PrimLongMapLI<PagedBTreeNode> getMemoryBuffer() {
        PrimLongMapLI<PagedBTreeNode> ret = new PrimLongMapLI<PagedBTreeNode>();
        ret.putAll(cleanBuffer);
        ret.putAll(dirtyBuffer);

		return ret;
	}

	/**
	 * This has to be called when a node changes its status from
	 * dirty to clean or vice versa.
	 */
	public void updatePageStatus(PagedBTreeNode node) {
		int pageId = node.getPageId();
		if(node.isDirty()) {
			cleanBuffer.remove(pageId);
			dirtyBuffer.put(pageId, node);
		} else {
			dirtyBuffer.remove(pageId);
			putInCleanBuffer(pageId, node);
		}
	}
	
	public PrimLongMapLI<PagedBTreeNode> getDirtyBuffer() {
		return dirtyBuffer;
	}

	public PrimLongMapLI<PagedBTreeNode> getCleanBuffer() {
		return cleanBuffer;
	}
	
    public int getStatNWrittenPages() {
		return statNWrittenPages;
	}

	public int getStatNReadPages() {
		return statNReadPages;
	}
	
	/**
	 * Iterates through tree and returns pageId of every reachable node
	 * that has been written to storage. Every non-reachable node should have
	 * been removed using BTree.remove and thus its page has been reported
	 * as free.
	 */
	public List<Integer> debugPageIds(PagedBTree tree) {
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

	public StorageChannel getStorageFile() {
		return this.storageFile;
	}
	
	public int getPageSize() {
		return this.pageSize;
	}
	
	public static int pageHeaderSize() {
		int nodeTypeIndicatorSize = 1;
		
		int size = 0;
		size += DiskIO.PAGE_HEADER_SIZE;
		size += nodeTypeIndicatorSize;
		
		return size;
	}

	@Override
	public int getNodeSizeInStorage(PagedBTreeNode node) {
		int size = 0;
		size += getNodeHeaderSizeInStorage(node);
		size += node.getNonKeyEntrySizeInBytes() + node.getKeyArraySizeInBytes();
		
		return size;
	}

    @Override
    public int getNodeHeaderSizeInStorage(PagedBTreeNode node) {
        return pageHeaderSize();
    }

	@Override
	public int getNodeValueElementSize() {
		return nodeValueElementSize;
	}
	
	public void setNodeValueElementSize(int sizeInByte) {
		nodeValueElementSize = sizeInByte;
	}

	public void setMaxCleanBufferElements(int maxCleanBufferElements) {
		this.maxCleanBufferElements = maxCleanBufferElements;
	}

	@Override
	public long getTxId() {
		return this.storageFile.getTxId();
	}
}