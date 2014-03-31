package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.LinkedList;

/**
 * Abstracts the need to specify a BTreeNodeFactory, which is specific to this type of tree.
 *
 * Also, adds the buffer manager that will be used by this type of node as an argument.
 */
public class PagedBTree extends BTree {

    private BTreeBufferManager bufferManager;

    public PagedBTree(int order, BTreeBufferManager bufferManager) {
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
    protected Pair<LinkedList<BTreeNode>, BTreeNode> searchNodeWithHistory(long key) {
        Pair<LinkedList<BTreeNode>, BTreeNode> result = super.searchNodeWithHistory(key);
        return new Pair<>(
                markListAsDirty(result.getA()),
                result.getB()
        );
    }

    @Override
    protected void markChanged(BTreeNode node) {
        PagedBTreeNode pagedNode = (PagedBTreeNode) node;
        pagedNode.markDirty();
    }

    private LinkedList<BTreeNode> markListAsDirty(LinkedList<BTreeNode> bTreeNodeList) {
        for (BTreeNode bTreeNode : bTreeNodeList) {
            PagedBTreeNode pagedBTreeNode = (PagedBTreeNode) bTreeNode;
            pagedBTreeNode.markDirty();
        }
        return bTreeNodeList;
    }

    public BTreeBufferManager getBufferManager() {
        return bufferManager;
    }
}
