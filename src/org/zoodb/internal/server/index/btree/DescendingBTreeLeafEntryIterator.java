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
            BTreeNode ancestorsChild = curLeaf;
            Integer position = (ancestors.size() == 0) ? 0 : ancestors.peek().getNumKeys();
            while (leftSibling == null && ancestors.size() > 0) {
                ancestor = ancestors.pop();
                position = positions.pop();

                //leftSibling = (T) ancestorsChild.leftSibling(ancestor);
                leftSibling = ancestor.leftSibling(position);
                position -= 1;
                ancestorsChild = ancestor;
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
        if (curLeaf != null && curLeaf.getKey(curPos) < start) {
            curLeaf = null;
        }
    }
    @Override
    void setFirstLeaf() {
        if (tree.isEmpty()) {
            return;
        }
        populateAncestorStack(end, Long.MAX_VALUE);
        // findKeyValuePos looks for a position to insert an entry
        // and thus it is one off
        curPos = curPos > 0 ? curPos-1 : 0; 
    
        // the following code is necessary for non unique trees,
        // because searchNodeWithHistory returns the correct node
        // for inserting an entry but here we need the last
        // entry whose key <= end.
	    while(this.hasNext() && curLeaf.getKey(curPos) > end) {
	    	this.next();
	    }
	    
	    // case when start is bigger than every element in the tree
        if(curLeaf!=null && start > curLeaf.getKey(curPos)) {
        	curLeaf = null;
        }
    }
}
