package org.zoodb.internal.server.index.btree;

public class PagedBTreeNode extends BTreeNode {

	private int pageId;
	private BTreeBufferManager bufferManager;

	private int[] childrenPageIds;
	private int parentPageId;

	private int leftPageId;
	private int rightPageId;

	public PagedBTreeNode(BTreeBufferManager bufferManager, BTreeNode parent,
			int order, boolean isLeaf) {
		super(parent, order, isLeaf);

		this.bufferManager = bufferManager;
		this.pageId = bufferManager.write(this);

		childrenPageIds = new int[order];
		setParent(parent);
	}

	public int getPageId() {
		return pageId;
	}

	@Override
	public BTreeNode getParent() {
        if (parentPageId == -1) {
            return null;
        }
		return bufferManager.read(parentPageId);
	}

	@Override
	public void setParent(BTreeNode parent) {
		this.parentPageId = nullSafeGetPageId(((PagedBTreeNode) parent));
	}

	@Override
	public BTreeNode[] getChildren() {
		BTreeNode[] children = new BTreeNode[order];
		int i = 0;
		for (; i < numKeys + 1; i++) {
			children[i] = bufferManager.read(childrenPageIds[i]);
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
				childrenPageIds[i] = ((PagedBTreeNode) children[i]).getPageId();
			}
		}
	}

	@Override
	public void setLeft(BTreeNode left) {
		this.leftPageId = nullSafeGetPageId(((PagedBTreeNode) left));
	}

	@Override
	public void setRight(BTreeNode right) {
		this.rightPageId = nullSafeGetPageId(((PagedBTreeNode) right));
	}

	@Override
	public BTreeNode getLeft() {
		return bufferManager.read(leftPageId);
	}

	@Override
	public BTreeNode getRight() {
		return bufferManager.read(rightPageId);
	}

	@Override
	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf) {
		return new PagedBTreeNode(bufferManager, parent, order, isLeaf);
	}

	@Override
	protected BTreeNode leftSiblingOf(BTreeNode node) {
		int index = 0;
		int nodePageId = ((PagedBTreeNode) node).getPageId();
		
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
		int nodePageId = ((PagedBTreeNode) node).getPageId();
		
		for (int childPageId : childrenPageIds) {
			if (childPageId == nodePageId) {
				if (index == childrenPageIds.length - 1) {
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
		childrenPageIds[index] = ((PagedBTreeNode) child).getPageId();
	}

	@Override
	public boolean equalChildren(BTreeNode other) {
        return arrayEquals(getChildren(), other.getChildren(), getNumKeys() + 1);
		//return arrayEquals(childrenPageIds, ((PagedBTreeNode) other).getChildrenPageIds(), numKeys + 1);
	}

	@Override
	public void copyChildren(BTreeNode source, int sourceIndex, BTreeNode dest,
			int destIndex, int size) {
		System.arraycopy(((PagedBTreeNode) source).getChildrenPageIds(), sourceIndex,
				((PagedBTreeNode) dest).getChildrenPageIds(), destIndex, size);
	}
	
	public int[] getChildrenPageIds() {
		return this.childrenPageIds;
	}
	
	int nullSafeGetPageId(PagedBTreeNode node) {
		return node!=null ? node.getPageId() : -1;
		
	}

}
