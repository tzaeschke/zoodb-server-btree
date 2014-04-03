package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;

/**
 * Corresponds to Unique B+ tree indices.
 */
public class UniquePagedBTreeNode extends PagedBTreeNode {

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
        super(bufferManager, order, isLeaf, isRoot);
    }

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, order, isLeaf, isRoot, pageId);
    }

    @Override
    public void initializeEntries(int order) {
        initKeys(order);
        if (isLeaf()) {
            initValues(order);
        } else {
            initChildren(order);
        }
    }

    @Override
    public UniquePagedBTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
        return new UniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
    }

    @Override
    public void migrateEntry(int destinationPos, BTreeNode source, int sourcePos) {
        long key = source.getKey(sourcePos);
        setKey(destinationPos, key);
    }

    /**
     * Leaf put.
     *
     * Requires that node is not full.
     *
     * @param key
     * @param value
     */
    public void put(long key, long value) {
        if (!isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }

        int pos = UniqueBTreeUtils.findKeyPos(this, key);
        if (pos > numKeys && getKey(pos) == key) {
            throw new IllegalStateException(
                    "Tree is not allowed to have non-unique keys.");
        }
        shiftRecords(pos, pos + 1, getNumKeys() - pos);
        setKey(pos, key);
        setValue(pos, value);
        incrementNumKyes();
    }

    @Override
    public void copyFromNodeToNode(int srcStartK, int srcStartC, BTreeNode destination, int destStartK, int destStartC, int keys, int children) {
        BTreeNode source = this;
        System.arraycopy(source.getKeys(), srcStartK, destination.getKeys(), destStartK, keys);
        if (destination.isLeaf()) {
            System.arraycopy(source.getValues(), srcStartK, destination.getValues(), destStartK, keys);
        } else {
            source.copyChildren(source, srcStartC, destination, destStartC, children);
        }
    }

    @Override
    public void shiftRecords(int startIndex, int endIndex, int amount) {
        shiftKeys(startIndex, endIndex, amount);
        if (isLeaf()) {
            shiftValues(startIndex, endIndex, amount);
        } else {
            shiftChildren(startIndex, endIndex, amount + 1);
        }
    }

    @Override
    public void shiftRecordsRight(int amount) {
        shiftKeys(0, amount, getNumKeys());
        if (isLeaf()) {
            shiftValues(0, amount, getNumKeys());
        } else {
            shiftChildren(0, amount, getNumKeys() + 1);
        }
    }

    @Override
    public void shiftRecordsLeftWithIndex(int startIndex, int amount) {
        int keysToMove = getNumKeys() - amount - startIndex;
        shiftKeys(startIndex + amount, startIndex, keysToMove);
        if (isLeaf()) {
            shiftValues(startIndex + amount, startIndex, keysToMove);
        } else {
            shiftChildren(startIndex + amount, startIndex, keysToMove + 1);
        }
    }

    @Override
    public String toString() {
        String ret = (isLeaf() ? "leaf" : "inner") + "-node: k:";
        ret += "[";
        for (int i = 0; i < this.getNumKeys(); i++) {
            ret += Long.toString(getKey(i));
            if (i != this.getNumKeys() - 1)
                ret += " ";
        }
        ret += "]";
        if (isLeaf()) {
            ret += ",   \tv:";
            ret += "[";
            for (int i = 0; i < this.getNumKeys(); i++) {
                ret += Long.toString(getValue(i));
                if (i != this.getNumKeys() - 1)
                    ret += " ";
            }
            ret += "]";
        } else {
            ret += "\n\tc:";
            if (this.getNumKeys() != 0) {
                for (int i = 0; i < this.getNumKeys() + 1; i++) {
                    String[] lines = this.getChild(i).toString()
                            .split("\r\n|\r|\n");
                    for (String l : lines) {
                        ret += "\n\t" + l;
                    }
                }
            }
        }
        return ret;
    }

}