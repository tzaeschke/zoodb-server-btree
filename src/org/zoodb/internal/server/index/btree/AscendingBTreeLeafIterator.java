package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

public class AscendingBTreeLeafIterator<T extends PagedBTreeNode> extends BTreeLeafIterator<T> {

    public AscendingBTreeLeafIterator(BTree tree) {
        super(tree);
    }

    public AscendingBTreeLeafIterator(BTree tree, long start, long end) {
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
        curPos = 0;
    }

    @Override
    void initializePosition() {
        curPos += 1;
        while (curLeaf != null && curLeaf.getKey(curPos) < start) {
            updatePosition();
        }
    }

}
