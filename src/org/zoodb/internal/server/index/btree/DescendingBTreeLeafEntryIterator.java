package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

public class DescendingBTreeLeafEntryIterator<T extends PagedBTreeNode> extends BTreeLeafEntryIterator<T> {

    public DescendingBTreeLeafEntryIterator(BTree<T> tree) {
        super(tree);
    }

    public DescendingBTreeLeafEntryIterator(BTree<T> tree, long start, long end) {
        super(tree, start, end);
    }

    @Override
    void updatePosition() {
        if (curPos > 0) {
            curPos--;
        } else {
            T leftSibling = null;
            T ancestor = null;
            T ancestorsChild = curLeaf;
            while (leftSibling == null && ancestors.size() > 0) {
                ancestor = ancestors.pop();
                leftSibling = (T) ancestorsChild.leftSibling(ancestor);
                ancestorsChild = ancestor;
            }
            ancestors.push(ancestor);
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
        Pair<LinkedList<T>, T> p = tree.searchNodeWithHistory(end, 0);
        ancestors = p.getA();
        curLeaf = p.getB();
        curPos = curLeaf.getNumKeys() - 1;
    }

    @Override
    void initializePosition() {
        curPos-=1;
        while (curLeaf != null && curLeaf.getKey(curPos) > end) {
            updatePosition();
        }
    }
}
