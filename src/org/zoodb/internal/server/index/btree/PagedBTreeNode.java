package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class PagedBTreeNode extends BTreeNode {

    private int pageId;
	private boolean isDirty;
	// isDirty: does the node in memory differ from the node in storage?
	private int[] childrenPageIds;
    protected BTreeBufferManager bufferManager;

    public PagedBTreeNode(int pageSize, boolean isLeaf, boolean isRoot) {
        super(pageSize, isLeaf, isRoot);
    }

	public PagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot) {
		super(pageSize, isLeaf, isRoot);
		
        markDirty();
		this.bufferManager = bufferManager;
		this.setPageId(bufferManager.save(this));
		init();
	}
	
	/*
	 * Constructor when we know on which page this node lies. 
	 * Does not save the node in the buffer managers memory.
	 */
    public PagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot, int pageId) {
		super(pageSize, isLeaf, isRoot);

		this.bufferManager = bufferManager;
		this.setPageId(pageId);
		init();
    }
    
    private void init() {
        if (bufferManager != null) {
            this.addObserver(bufferManager);
        }
    }

    @Override
    public boolean willOverflowAfterInsert(long key, long value) {
        if (getNumKeys() == 0) {
            return false;
        }

        //check if the node contains the key (unique tree) or key/value pair (non/unique tree)
        int pos = this.findKeyValuePos(key, value);
        if(checkNonUniqueKey(pos, key) && (!allowNonUniqueKeys() || checkNonUniqueKeyValue(pos,key,value))) {
            return false;
        }

        //recompute the prefix and check the new size
        long first = Math.min(getSmallestKey(), key);
        long last = Math.max(getLargestKey(), key);
        int newNumKeys = getNumKeys() + 1;
        long prefix = PrefixSharingHelper.computePrefix(first, last);
        long keyArrayAfterInsertSizeInBytes = PrefixSharingHelper.encodedArraySize(newNumKeys, prefix);
        int newPageSize = (int) (storageHeaderSize() + (keyArrayAfterInsertSizeInBytes + getNonKeyEntrySizeInBytes(newNumKeys)));

        boolean willOverflow = pageSize <= newPageSize;
        return willOverflow;
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
    protected void initChildren(int size) {
        //This is called by the BTreeNode constructor
        this.childrenPageIds = new int[size];
    }

	@Override
	public BTreeNode[] getChildren() {
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
		return bufferManager.read(childrenPageIds[index]);
	}

	@Override
	public void setChild(int index, BTreeNode child) {
		markDirty();
		childrenPageIds[index] = toPagedNode(child).getPageId();
	}

    @Override
    public <T extends BTreeNode> void removeChild(T child) {
        PagedBTreeNode pagedChild = toPagedNode(child);
        int childPageId = pagedChild.getPageId();
        int childIndex = -1;
        for (int i = 0; i < getNumKeys() + 1; i++) {
            if (childrenPageIds[i] == childPageId) {
                childIndex = i;
                break;
            }
        }
        if (childIndex == this.getNumKeys()) {
            this.decrementNumKeys();
        } else {
            long oldKey = this.getKey(childIndex);
            this.shiftRecordsLeftWithIndex(childIndex, 1);
            if (childIndex > 0) {
                this.setKey(childIndex - 1, oldKey);
            }
            this.decrementNumKeys();
        }
    }

    @Override
	public boolean equalChildren(BTreeNode other) {
		if(getNumKeys() > 0) {
            return arrayEquals(getChildren(), other.getChildren(), getNumKeys() + 1);
		} else {
			return getNumKeys() == other.getNumKeys();
		}
	}

	@Override
	public void copyChildren(BTreeNode source, int sourceIndex, BTreeNode dest,
			int destIndex, int size) {
		System.arraycopy(toPagedNode(source).getChildrenPageIds(), sourceIndex,
				toPagedNode(dest).getChildrenPageIds(), destIndex, size);
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

    @Override
    public int keyIndexOf(BTreeNode left, BTreeNode right) {
        int leftPageId = toPagedNode(left).getPageId();
        int rightPageId = toPagedNode(right).getPageId();
        int indexRight = 0;
        for (int pageId : childrenPageIds) {
            if (pageId == rightPageId) {
                if (indexRight > 0 && childrenPageIds[indexRight-1] == leftPageId) {
                    return indexRight - 1;
                }
            }
            indexRight++;
        }
        throw new RuntimeException("Wrong call for parent index");

    }

    /*
     * Mark this node dirty which must mark all parents up to the root dirty as
     * well because they depend on this node.
     */
	public void markDirty() {
		if (isDirty()) {
			// this node is dirty, so parents must be already dirty
			return;
		}
		isDirty = true;
        //TODO mark parents as dirty as well

		setChanged();
		notifyObservers();
	}

	public void markClean() {
		isDirty = false;

		setChanged();
		notifyObservers();
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
		bufferManager.remove(getPageId());
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
}
