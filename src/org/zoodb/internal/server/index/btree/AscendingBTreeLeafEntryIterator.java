package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

public class AscendingBTreeLeafEntryIterator<T extends BTreeNode> extends BTreeLeafEntryIterator<T> {

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
            T rightSibling = null;
            T ancestor = null;
            T ancestorsChild = curLeaf;
            while (rightSibling == null && ancestors.size() > 0) {
                ancestor = ancestors.pop();
                rightSibling = (T) ancestorsChild.rightSibling(ancestor);
                ancestorsChild = ancestor;
            }
            ancestors.push(ancestor);
            if (rightSibling == null) {
                curLeaf = null;
            } else {
                curLeaf = getLefmostLeaf(rightSibling);
            }
        }
        if (curLeaf != null && curLeaf.getKey(curPos) > end) {
            curLeaf = null;
        }
    }

    void setFirstLeaf() {
        if (tree.isEmpty()) {
            return;
        }
        Pair<LinkedList<T>, T> p = tree.searchNodeWithHistory(start, 0);
        ancestors = p.getA();
        curLeaf = p.getB();
        curPos = curLeaf.findKeyValuePos(start, 0);
        curPos = curPos > 0 ? curPos-1 : 0; 
    }

}
