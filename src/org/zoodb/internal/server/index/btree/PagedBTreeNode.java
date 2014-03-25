package org.zoodb.internal.server.index.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.zoodb.internal.util.Pair;

public class PagedBTreeNode extends BTreeNode {

	private int pageId;
	private BTreeBufferManager bufferManager;
	private boolean isDirty;
	// isDirty: does the node in memory differ from the node in storage?

	private int[] childrenPageIds;
	private int parentPageId;

	private int leftPageId;
	private int rightPageId;

	public PagedBTreeNode(BTreeBufferManager bufferManager, BTreeNode parent,
			int order, boolean isLeaf) {
		super(parent, order, isLeaf);

		this.bufferManager = bufferManager;
		this.setPageId(bufferManager.save(this));

		childrenPageIds = new int[order];
		markDirty();
	}

	public void put(long key, long value) {
		markDirty();
		super.put(key, value);
	}

	public void put(long key, BTreeNode newNode) {
		markDirty();
		super.put(key, newNode);
	}

	public void put(long key, BTreeNode left, BTreeNode right) {
		markDirty();
		super.put(key, left, right);
	}

	public BTreeNode putAndSplit(long newKey, long value) {
		markDirty();
		return super.putAndSplit(newKey, value);
	}

	public Pair<BTreeNode, Long> putAndSplit(long key, BTreeNode newNode) {
		markDirty();
		return super.putAndSplit(key, newNode);
	}

	public void delete(long key) {
		markDirty();
		super.delete(key);
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
	public BTreeNode getParent() {
		if (parentPageId == -1) {
			return null;
		}
		return bufferManager.read(parentPageId);
	}

	@Override
	public void setParent(BTreeNode parent) {
		this.parentPageId = nullSafeGetPageId(toPagedNode(parent));
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
				childrenPageIds[i] = toPagedNode(children[i]).getPageId();
			}
		}
	}

	@Override
	public void setLeft(BTreeNode left) {
		markDirty();
		this.leftPageId = nullSafeGetPageId(toPagedNode(left));
	}

	@Override
	public void setRight(BTreeNode right) {
		markDirty();
		this.rightPageId = nullSafeGetPageId(toPagedNode(right));
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
		markDirty();
		childrenPageIds[index] = toPagedNode(child).getPageId();
	}

	@Override
	public boolean equalChildren(BTreeNode other) {
		return arrayEquals(getChildren(), other.getChildren(), getNumKeys() + 1);
	}

	@Override
	public void copyChildren(BTreeNode source, int sourceIndex, BTreeNode dest,
			int destIndex, int size) {
		System.arraycopy(toPagedNode(source).getChildrenPageIds(), sourceIndex,
				toPagedNode(dest).getChildrenPageIds(), destIndex, size);
	}

	private int[] getChildrenPageIds() {
		return this.childrenPageIds;
	}

	public List<Integer> getChildrenPageIdList() {
		List<Integer> childrenPageIdList = new ArrayList<Integer>(getNumKeys()+1);
		for(int i=0; i<getNumKeys()+1; i++) {
			childrenPageIdList.add(childrenPageIds[i]);
		}
		return childrenPageIdList;
	}

	public boolean isDirty() {
		return isDirty;
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

		PagedBTreeNode parent = toPagedNode(getParent());
		if (parent != null) {
			parent.markDirty();
		}
	}

	public void markClean() {
		isDirty = false;
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
}
