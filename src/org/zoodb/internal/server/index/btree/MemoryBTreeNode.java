package org.zoodb.internal.server.index.btree;

public class MemoryBTreeNode extends BTreeNode {

	private BTreeNode[] children;
	private BTreeNode parent;

	private BTreeNode left;
	private BTreeNode right;

	public MemoryBTreeNode(BTreeNode parent, int order, boolean isLeaf) {
		super(parent, order, isLeaf);
		// TODO Auto-generated constructor stub
	}

	public BTreeNode[] getChildren() {
		return children;
	}

	public void setChildren(BTreeNode[] children) {
		this.children = children;
	}

	public BTreeNode getParent() {
		return parent;
	}

	public void setParent(BTreeNode parent) {
		this.parent = parent;
	}

	public BTreeNode getLeft() {
		return left;
	}

	public void setLeft(BTreeNode left) {
		this.left = left;
	}

	public BTreeNode getRight() {
		return right;
	}

	public void setRight(BTreeNode right) {
		this.right = right;
	}

	public BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf) {
		return new MemoryBTreeNode(parent, order, isLeaf);

	}

}
