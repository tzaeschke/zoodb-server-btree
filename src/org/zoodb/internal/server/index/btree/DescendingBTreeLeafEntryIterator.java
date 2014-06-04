/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 *
 * This file is part of ZooDB.
 *
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 *
 * See the README and COPYING files for further information.
 */
package org.zoodb.internal.server.index.btree;

/**
 * A descending iterator for the entries in the leaf nodes of the B+ tree.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
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
