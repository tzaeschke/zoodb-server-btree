package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.*;
import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

/**
 * Abstracts the need to specify a BTreeNodeFactory, which is specific to this type of tree.
 *
 * Also, adds the buffer manager that will be used by this type of node as an argument.
 */
public class UniquePagedBTree extends UniqueBTree<PagedBTreeNode> {

    private BTreeBufferManager bufferManager;

    public UniquePagedBTree(int order, BTreeBufferManager bufferManager) {
        super(order, new PagedBTreeNodeFactory(bufferManager));
        this.bufferManager = bufferManager;
    }

    @Override
    public void delete(long key) {
        //TODO need to all nodes involved in delete as dirty, not just the path down
        //ex: nodes from which entries are borrowed and merged nodes
        super.delete(key);
    }

    /**
     * Overwrites the search node method to mark all the ancestor nodes as dirty.
     *
     * Should work properly for insertion and simple cases of deletion, where no splitting and merging is done.
     * @param key
     * @return
     */
    @Override
    protected Pair<LinkedList<PagedBTreeNode>, PagedBTreeNode> searchNodeWithHistory(long key) {
        Pair<LinkedList<PagedBTreeNode>, PagedBTreeNode> result = super.searchNodeWithHistory(key);
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
