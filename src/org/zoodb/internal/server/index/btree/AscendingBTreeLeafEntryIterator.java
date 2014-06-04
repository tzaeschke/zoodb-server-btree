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
 * An ascending iterator for the entries in the leaf nodes of the B+ tree.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
public class AscendingBTreeLeafEntryIterator extends BTreeLeafEntryIterator {

    public AscendingBTreeLeafEntryIterator(BTree tree) {
        super(tree);
    }

    public AscendingBTreeLeafEntryIterator(BTree tree, long start, long end) {
        super(tree, start, end);
    }

    void updatePosition() {
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
