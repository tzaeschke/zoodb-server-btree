package org.zoodb.internal.server.index.btree;

public class NonUniquePagedBTreeNode extends PagedBTreeNode {

    public NonUniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
        super(bufferManager, order, isLeaf, isRoot);
    }

    public NonUniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, order, isLeaf, isRoot, pageId);
    }

    @Override
    public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
        return new NonUniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
    }


}
