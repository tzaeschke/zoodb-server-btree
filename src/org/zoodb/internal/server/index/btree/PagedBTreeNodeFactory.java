package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTreeNode;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTreeNode;

public class PagedBTreeNodeFactory implements BTreeNodeFactory {

	private BTreeBufferManager bufferManager;

	public PagedBTreeNodeFactory(BTreeBufferManager bufferManager) {
		this.bufferManager = bufferManager;
	}

	@Override
	public BTreeNode newUniqueNode(int order, boolean isLeaf, boolean isRoot) {
		return new UniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
	}

    @Override
    public BTreeNode newNonUniqueNode(int order, boolean isLeaf, boolean isRoot) {
        return new NonUniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
    }

    public static PagedBTreeNode constructLeaf( BTreeBufferManager bufferManager,
                                                boolean isUnique,
                                                boolean isRoot,
                                                int order,
                                                int pageId,
                                                int numKeys,
                                                long[] keys,
                                                long[] values) {
        boolean isLeaf = true;
        PagedBTreeNode node = createNode(bufferManager, isUnique, isRoot, isLeaf, order, pageId);

		node.setNumKeys(numKeys);
		node.setKeys(keys);
		node.setValues(values);
		return node;
	}
	
    public static PagedBTreeNode constructInnerNode( BTreeBufferManager bufferManager,
                                                     boolean isUnique,
                                                     boolean isRoot,
                                                     int order,
                                                     int pageId,
                                                     int numKeys,
                                                     long[] keys,
                                                     int[] childrenPageIds) {
        boolean isLeaf = false;
		PagedBTreeNode node = createNode(bufferManager, isUnique, isRoot, isLeaf, order, pageId);

		node.setNumKeys(numKeys);
		node.setKeys(keys);
		node.setChildrenPageIds(childrenPageIds);
		return node;
	}

    private static PagedBTreeNode createNode(   BTreeBufferManager bufferManager,
                                                boolean isUnique,
                                                boolean isRoot,
                                                boolean isLeaf,
                                                int order,
                                                int pageId) {
        PagedBTreeNode node;
        if (isUnique) {
            node = new UniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot, pageId);
        } else {
            node = new NonUniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot, pageId);
        }
        return node;
    }

}
