package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

public class AscendingBTreeLeafIterator<T extends PagedBTreeNode> extends BTreeLeafIterator<T> {

    public AscendingBTreeLeafIterator(BTree tree) {
        super(tree);
    }

    public AscendingBTreeLeafIterator(BTree tree, int start, int end) {
        super(tree, start, end);
    }

    void updatePosition() {
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
                if (!rightSibling.isLeaf())
                    ancestors.push(rightSibling);
                curLeaf = (T) rightSibling.leftMostLeafOf();
            }
        }
    }

    void setFirstLeaf() {
        if (tree.isEmpty()) {
            return;
        }
        Pair<LinkedList<T>, T> p = tree.searchNodeWithHistory(start, 0);
        ancestors = p.getA();
        curLeaf = p.getB();
        curPos = 0;
    }

}
