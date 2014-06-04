package org.zoodb.internal.server.index.btree;

public class DescendingBTreeLeafEntryIterator extends BTreeLeafEntryIterator {

    public DescendingBTreeLeafEntryIterator(BTree tree) {
        super(tree);
    }

    public DescendingBTreeLeafEntryIterator(BTree tree, long start, long end) {
        super(tree, start, end);
    }

    @Override
    void updatePosition() {
        if (curPos > 0) {
            curPos--;
        } else {
            BTreeNode leftSibling = null;
            BTreeNode ancestor = null;
            Integer position = (ancestors.size() == 0) ? 0 : ancestors.peek().getNumKeys();
            while (leftSibling == null && ancestors.size() > 0) {
                ancestor = ancestors.pop();
                position = positions.pop();

                //leftSibling = (T) ancestorsChild.leftSibling(ancestor);
                leftSibling = ancestor.leftSibling(position);
                position --;
            }
            ancestors.push(ancestor);
            positions.push(position);
            if (leftSibling == null) {
                curLeaf = null;
            } else {
                curLeaf = getRightMostLeaf(leftSibling);
                curPos = curLeaf.getNumKeys() - 1;
            }
        }
        if (curLeaf != null && curLeaf.getKey(curPos) < min) {
            curLeaf = null;
        }
    }
    @Override
    void setFirstLeaf() {
        if (tree.isEmpty()) {
            return;
        }
        
        populateAncestorStack(max, Long.MAX_VALUE);
    
        // the following code is necessary for non unique trees,
        // because searchNodeWithHistory returns the correct node
        // for inserting an entry but here we need the last
        // entry whose key <= max.
	    while (curLeaf != null && curLeaf.getKey(curPos) > max) {
	    	this.next();
	    }
	    
	    // case when min is bigger than every element in the tree
        if (curLeaf != null && min > curLeaf.getKey(curPos)) {
        	curLeaf = null;
        }
    }
}
