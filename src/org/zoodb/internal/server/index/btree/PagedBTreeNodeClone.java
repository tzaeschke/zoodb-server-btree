package org.zoodb.internal.server.index.btree;

import java.util.Arrays;

public class PagedBTreeNodeClone {

    private int pageId;
    private long[] keys;
    private long[] values;

    public PagedBTreeNodeClone(PagedBTreeNode pagedBTreeNode) {
        this.pageId = pagedBTreeNode.getPageId();
        int numKeys = pagedBTreeNode.getNumKeys();
        this.keys = Arrays.copyOf(pagedBTreeNode.getKeys(), numKeys);
        if (pagedBTreeNode.getValues() != null){
            this.values = Arrays.copyOf(pagedBTreeNode.getValues(), numKeys);
        }
    }
}
