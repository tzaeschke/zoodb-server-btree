package org.zoodb.internal.server.index.btree;

import java.util.ArrayList;
import java.util.List;

public abstract class PagedBTreeNode extends BTreeNode {

	private int pageId;
	private boolean isDirty;
	// isDirty: does the node in memory differ from the node in storage?
	private int[] childrenPageIds;
    protected BTreeBufferManager bufferManager;

	public PagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
		super(order, isLeaf, isRoot);

		markDirty();
		this.bufferManager = bufferManager;
		this.setPageId(bufferManager.save(this));
		this.addObserver(bufferManager);

		childrenPageIds = new int[order];
	}
	
	/*
	 * Constructor when we know on which page this node lies.
	 */
    public PagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
		super(order, isLeaf, isRoot);

		this.bufferManager = bufferManager;
		this.setPageId(pageId);
		
		childrenPageIds = new int[order];
    }

	public void setKey(int index, long key) {
		markDirty();
		super.setKey(index, key);
	}

	public void setValue(int index, long value) {
		markDirty();
		super.setValue(index, value);
	}

	public void setKeys(long[] keys) {
		// markDirty(); would be called before buffer manager is ready
		super.setKeys(keys);
	}

	public void setValues(long[] values) {
		// markDirty(); would be called before buffer manager is ready
		super.setValues(values);
	}

	@Override
	public BTreeNode[] getChildren() {
		BTreeNode[] children = new BTreeNode[order];
		int i = 0;
		if(numKeys > 0) {
            for (; i < numKeys + 1; i++) {
                    children[i] = bufferManager.read(childrenPageIds[i]);
            }
        }
		for (; i < order; i++) {
			children[i] = null;
		}
		return children;
	}

	@Override
	public void setChildren(BTreeNode[] children) {
		childrenPageIds = new int[order];

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
			return new ArrayList<Integer>(0);
		}

		List<Integer> childrenPageIdList = new ArrayList<Integer>(getNumKeys()+1);

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



	@Override
	public void close() {
		bufferManager.remove(getPageId());
	}

}