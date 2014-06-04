package org.zoodb.internal.server.index.btree;

public class AscendingBTreeLeafEntryIterator extends BTreeLeafEntryIterator {

    public AscendingBTreeLeafEntryIterator(BTree tree) {
        super(tree);
    }

    public AscendingBTreeLeafEntryIterator(BTree tree, long start, long end) {
        super(tree, start, end);
    }

    void updatePosition() {
        //TODO fix this
        if (curPos < curLeaf.getNumKeys() - 1) {
            curPos++;
        } else {
            curPos = 0;
            BTreeNode rightSibling = null;
            BTreeNode ancestor = null;
            Integer position = 0;
            while (rightSibling == null && ancestors.size() > 0) {
                ancestor = ancestors.pop();
                position = positions.pop();
                rightSibling = ancestor.rightSibling(position);
                position ++;
            }
            ancestors.push(ancestor);
            positions.push(position);
            if (rightSibling == null) {
                curLeaf = null;
            } else {
                curLeaf = getLefmostLeaf(rightSibling);
            }
        }
        if (curLeaf != null && curLeaf.getKey(curPos) > max) {
            curLeaf = null;
        }
    }

    void setFirstLeaf() {
        if (tree.isEmpty()) {
            return;
        }

        populateAncestorStack(min, Long.MIN_VALUE);
        
        // the following code is necessary for non unique trees,
        // because searchNodeWithHistory returns the correct node
        // for inserting an entry but here we need the first
        // entry whose key >= min.
        while (curLeaf != null && curLeaf.getKey(curPos) < min) {
        	this.next();
        }
        
	    // case when max is smaller than every element in the tree
        if (curLeaf != null && max < curLeaf.getKey(curPos)) {
        	curLeaf = null;
        }
    }
}
