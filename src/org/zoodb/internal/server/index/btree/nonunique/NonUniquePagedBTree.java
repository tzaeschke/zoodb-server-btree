package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNodeFactory;
import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

public class NonUniquePagedBTree extends NonUniqueBTree<PagedBTreeNode> {

    private BTreeBufferManager bufferManager;

    public NonUniquePagedBTree(int order, BTreeBufferManager bufferManager) {
        super(order, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    @Override
    protected Pair<LinkedList<PagedBTreeNode>, PagedBTreeNode> searchNodeWithHistory(long key, long value) {
        //TODO explicitly mark nodes as dirty during the operations
        Pair<LinkedList<PagedBTreeNode>, PagedBTreeNode> result = super.searchNodeWithHistory(key, value);
        return new Pair<>(
                markListAsDirty(result.getA()),
                result.getB()
        );
    }

    private LinkedList<PagedBTreeNode> markListAsDirty(LinkedList<PagedBTreeNode> bTreeNodeList) {
        for (PagedBTreeNode bTreeNode : bTreeNodeList) {
            PagedBTreeNode pagedBTreeNode = bTreeNode;
            pagedBTreeNode.markDirty();
        }
        return bTreeNodeList;
    }

    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }

}
