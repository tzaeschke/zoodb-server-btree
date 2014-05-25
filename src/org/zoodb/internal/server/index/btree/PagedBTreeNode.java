package org.zoodb.internal.server.index.btree;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

public abstract class PagedBTreeNode extends BTreeNode {

    private int pageId;
	private boolean isDirty;
	// isDirty: does the node in memory differ from the node in storage?
	private int[] childrenPageIds;
    protected BTreeBufferManager bufferManager;
    private WeakReference<PagedBTreeNode>[] children;

	public PagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot) {
		super(pageSize, isLeaf, isRoot, bufferManager.getNodeValueElementSize());
		
        markDirty();
		this.bufferManager = bufferManager;
		this.setPageId(bufferManager.save(this));
	}
	
	/*
	 * Constructor when we know on which page this node lies. 
	 * Does not save the node in the buffer managers memory.
	 */
    public PagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot, int pageId) {
		super(pageSize, isLeaf, isRoot, bufferManager.getNodeValueElementSize());

		this.bufferManager = bufferManager;
		this.setPageId(pageId);
    }
    
    @Override
    public boolean fitsIntoOneNodeWith(BTreeNode neighbour) {
        if (neighbour == null) {
            return false;
        }
        if (neighbour.getNumKeys() == 0 || this.getNumKeys() == 0) {
            return true;
        }
        long first = Math.min(this.getSmallestKey(), neighbour.getSmallestKey());
        long last = Math.max(this.getLargestKey(), neighbour.getLargestKey());
        int newNumKeys = this.getNumKeys() + neighbour.getNumKeys();
        if (!this.isLeaf()) {
            //also take into account the key that is taken down from the parent
            //ToDo check if this is always needed
            newNumKeys += 1;
        }
        //ToDo move to field
        long prefix = PrefixSharingHelper.computePrefix(first, last);
        long keyArrayAfterInsertSizeInBytes = PrefixSharingHelper.encodedArraySize(newNumKeys, prefix);
        int newPageSize = (int) (storageHeaderSize() + (keyArrayAfterInsertSizeInBytes + getNonKeyEntrySizeInBytes(newNumKeys)));
        boolean willNotOverflow = pageSize >= newPageSize;
        return willNotOverflow;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void initChildren(int size) {
        //This is called by the BTreeNode constructor
        this.childrenPageIds = new int[size];
        this.childSizes = new int[size];
        this.children = new WeakReference[size];
    }

	@Override
	public BTreeNode[] getChildNodes() {
        //this method shouldn't really be used in development
        //as it reads all the children from disk
		BTreeNode[] children = new BTreeNode[childrenPageIds.length];
		int i = 0;
		if(numKeys > 0) {
            for (; i < numKeys + 1; i++) {
                    children[i] = bufferManager.read(childrenPageIds[i]);
            }
        }
		for (; i < childrenPageIds.length; i++) {
			children[i] = null;
		}
		return children;
	}

	@Override
	public void setChildren(BTreeNode[] children) {
		childrenPageIds = new int[children.length];

		for (int i = 0; i < children.length; i++) {
			if (children[i] != null) {
				childrenPageIds[i] = toPagedNode(children[i]).getPageId();
			}
		}
	}

	@Override
	protected BTreeNode leftSiblingOf(BTreeNode node) {
		int index = 0;
		int nodePageId = toPagedNode(node).getPageId();

		for (int childPageId : childrenPageIds) {
			if (childPageId == nodePageId) {
				if (index == 0) {
					return null;
				} else {
					return getChild(index - 1);
				}
			}
			index++;
		}
		return null;
	}

	@Override
	protected BTreeNode rightSiblingOf(BTreeNode node) {
		int index = 0;
		int nodePageId = toPagedNode(node).getPageId();

		for (int childPageId : childrenPageIds) {
			if (childPageId == nodePageId) {
				if (index == getNumKeys() || index == childrenPageIds.length - 1) {
					return null;
				} else {
					return getChild(index + 1);
				}
			}
			index++;
		}
		return null;
	}

	@Override
	public BTreeNode getChild(int index) {
        PagedBTreeNode child;
        if (children[index] == null || children[index].get() == null)  {
            child = bufferManager.read(childrenPageIds[index]);
            children[index] = new WeakReference<>(child);
            return child;
        }

		return children[index].get();
	}

	@Override
	public void setChild(int index, BTreeNode child) {
		markDirty();
        PagedBTreeNode pagedChild = toPagedNode(child);
		childrenPageIds[index] = pagedChild.getPageId();
        children[index] = new WeakReference<>(pagedChild);
        childSizes[index] = pagedChild.getCurrentSize();
	}

    @Override
	public boolean equalChildren(BTreeNode other) {
		if(getNumKeys() > 0) {
            return arrayEquals(getChildNodes(), other.getChildNodes(), getNumKeys() + 1);
		} else {
			return getNumKeys() == other.getNumKeys();
		}
	}

	@Override
	public void copyChildren(BTreeNode source, int sourceIndex, BTreeNode dest,
			int destIndex, int size) {
        PagedBTreeNode pagedSource = toPagedNode(source);
        PagedBTreeNode pagedDest = toPagedNode(dest);
		System.arraycopy(pagedSource.getChildrenPageIds(), sourceIndex,
				pagedDest.getChildrenPageIds(), destIndex, size);
        System.arraycopy(pagedSource.getChildSizes(), sourceIndex, pagedDest.getChildSizes(), destIndex, size);
        System.arraycopy(pagedSource.getChildren(), sourceIndex, pagedDest.getChildren(), destIndex, size);
	}

	public int[] getChildrenPageIds() {
		return this.childrenPageIds;
	}

	public List<Integer> getChildrenPageIdList() {
		if(getNumKeys() == 0) {
			return new ArrayList<>(0);
		}

		List<Integer> childrenPageIdList = new ArrayList<>(getNumKeys()+1);

		for(int i=0; i<getNumKeys()+1; i++) {
			childrenPageIdList.add(childrenPageIds[i]);
		}
		return childrenPageIdList;
	}

	public boolean isDirty() {
		return isDirty;
	}

    @Override
    public void markChanged() {
        this.markDirty();
    }

	public void markDirty() {
		isDirty = true;
		notifyStatus();
	}

	public void markClean() {
		isDirty = false;
		notifyStatus();
	}
	
	private void notifyStatus() {
		if(this.bufferManager != null) {
			this.bufferManager.updatePageStatus(this);
		}
	}

	public int getPageId() {
		return pageId;
	}

	private int nullSafeGetPageId(PagedBTreeNode node) {
		return node != null ? node.getPageId() : -1;

	}
	private static PagedBTreeNode toPagedNode(BTreeNode node) {
		return (PagedBTreeNode) node;
	}

	public void setPageId(int pageId) {
		this.pageId = pageId;
	}

	public void setChildPageId(int childIndex, int childPageId) {
		childrenPageIds[childIndex] = childPageId;
		markDirty();
	}

	public void setChildrenPageIds(int[] childrenPageIds) {
		this.childrenPageIds = childrenPageIds;
	}

    public void cloneInto(PagedBTreeNode clone) {
        clone.setPageId(this.getPageId());
        clone.setKeys(Arrays.copyOf(this.getKeys(), this.getKeys().length));
        copyValues(clone);
    }

    protected abstract void copyValues(PagedBTreeNode node);

	@Override
	public void close() {
		bufferManager.remove(this);
	}

    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }

    @Override
    public int computeSize() {
        return bufferManager.getNodeSizeInStorage(this);
    }

    @Override
    public int storageHeaderSize() {
        return bufferManager.getNodeHeaderSizeInStorage(this);
    }

    public WeakReference<PagedBTreeNode>[] getChildren() {
        return children;
    }

    public static int computeMaxPossibleEntries(boolean isUnique, boolean isLeaf, int pageSize, int valueElementSize) {
        //ToDo use this same method in the node, to compute the sizes on init
        int maxPossibleNumEntries;
        /*
            In the case of the best compression, all keys would have the same value.
         */
        int encodedKeyArraySize = PrefixSharingHelper.SMALLEST_POSSIBLE_COMPRESSION_SIZE;

        if (isLeaf) {
            //subtract a 64 bit prefix and divide by 8 (the number of bytes in a long)
            maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) / valueElementSize) + 1;
        } else {
            //inner nodes also contain children ids which are ints
            //need to divide by 4
            //n * 4
            if (isUnique) {
                maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) >>> 2 ) + 1;
            } else {
                //ToDo this might not be right
                maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) / 12 ) + 1;
            }

        }
        return maxPossibleNumEntries;
    }
    
}
