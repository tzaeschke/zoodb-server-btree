package org.zoodb.internal.server.index.btree;

public class MemoryBTreeNode extends BTreeNode {

	private BTreeNode[] children;
	private BTreeNode parent;

	public MemoryBTreeNode(int order, boolean isLeaf, boolean isRoot) {
		super(order, isLeaf, isRoot);
		// TODO Auto-generated constructor stub
	}

	public BTreeNode[] getChildren() {
		return children;
	}

	public void setChildren(BTreeNode[] children) {
		this.children = children;
	}

    @Override
    public void markChanged() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public BTreeNode getParent() {
		return parent;
	}

	public void setParent(BTreeNode parent) {
		this.parent = parent;
	}

	public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
		return new MemoryBTreeNode(order, isLeaf, isRoot);

	}

	public boolean equalChildren(BTreeNode other) {
		return arrayEquals(getChildren(), other.getChildren(), getNumKeys() + 1);
	}

	public BTreeNode getChild(int index) {
		return getChildren()[index];
	}

	public void setChild(int index, BTreeNode child) {
		getChildren()[index] = child;
	}

	protected BTreeNode leftSiblingOf(BTreeNode node) {
		int index = 0;
		for (BTreeNode child : getChildren()) {
			if (child == node) {
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

	protected BTreeNode rightSiblingOf(BTreeNode node) {
		int index = 0;
		for (BTreeNode child : getChildren()) {
			if (child == node) {
				if (index == getNumKeys()) {
					return null;
				} else {
					return getChild(index + 1);
				}
			}
			index++;
		}
		return null;
	}

	public void copyChildren(BTreeNode src, int srcPos,
			BTreeNode dest, int destPos, int length) {
		System.arraycopy(src.getChildren(), srcPos, dest.getChildren(), destPos, length);
	}

    @Override
    public <T extends BTreeNode> void put(long key, long value) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
