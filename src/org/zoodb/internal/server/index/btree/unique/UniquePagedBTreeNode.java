package org.zoodb.internal.server.index.btree.unique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

/**
 * Corresponds to Unique B+ tree indices.
 */
public class UniquePagedBTreeNode extends PagedBTreeNode {

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot) {
        super(bufferManager, pageSize, isLeaf, isRoot);
    }

    public UniquePagedBTreeNode(BTreeBufferManager bufferManager, int pageSize, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, pageSize, isLeaf, isRoot, pageId);
    }

    @Override
    protected void copyValues(PagedBTreeNode node) {
        //do nothing
    }

    @Override
    public void initializeEntries() {
        int size = computeMaxPossibleNumEntries();
        initKeys(size);
        if (!isLeaf()) {
            initChildren(size + 1);
        } else {
            initValues(size);
        }
    }

    @Override
    public UniquePagedBTreeNode newNode(int pageSize, boolean isLeaf, boolean isRoot) {
        return new UniquePagedBTreeNode(bufferManager, pageSize, isLeaf, isRoot);
    }

    @Override
    public void migrateEntry(int destinationPos, BTreeNode source, int sourcePos) {
        long key = source.getKey(sourcePos);
        setKey(destinationPos, key);

        markDirty();
    }

    @Override
    public void setEntry(int pos, long key, long value) {
        setKey(pos, key);
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
        
        markDirty();
    }

    @Override
    public void shiftRecords(int startIndex, int endIndex, int amount) {
        shiftKeys(startIndex, endIndex, amount);
        if (isLeaf()) {
            shiftValues(startIndex, endIndex, amount);
        } else {
            shiftChildren(startIndex, endIndex, amount + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsRight(int amount) {
        shiftKeys(0, amount, getNumKeys());
        if (isLeaf()) {
            shiftValues(0, amount, getNumKeys());
        } else {
            shiftChildren(0, amount, getNumKeys() + 1);
        }
        
        markDirty();
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
        
        markDirty();
    }

    @Override
    protected boolean containsAtPosition(int position, long key, long value) {
        return this.getKey(position) == key;
    }

    @Override
    protected boolean smallerThanKeyValue(int position, long key, long value) {
        return (key < getKey(position));
    }
    
    @Override
    protected boolean allowNonUniqueKeys() {
    	return false;
    }

    @Override
    public String toString() {
        String ret = (isLeaf() ? "leaf" : "inner") + "-node: k:";
//        ret += "[";
//        for (int i = 0; i < this.getNumKeys(); i++) {
//            ret += Long.toString(getKey(i));
//            if (i != this.getNumKeys() - 1)
//                ret += " ";
//        }
//        ret += "]";
//        if (isLeaf()) {
//            ret += ",   \tv:";
//            ret += "[";
//            for (int i = 0; i < this.getNumKeys(); i++) {
//                ret += Long.toString(getValue(i));
//                if (i != this.getNumKeys() - 1)
//                    ret += " ";
//            }
//            ret += "]";
//        } else {
//            ret += "\n\tc:";
//            if (this.getNumKeys() != 0) {
//                for (int i = 0; i < this.getNumKeys() + 1; i++) {
//                    String[] lines = this.getChild(i).toString()
//                            .split("\r\n|\r|\n");
//                    for (String l : lines) {
//                        ret += "\n\t" + l;
//                    }
//                }
//            }
//        }
        return ret;
    }

    @Override
    public int computeMaxPossibleNumEntries() {
        int maxPossibleNumEntries;
        /*
            In the case of the best compression, all keys would have the same value.
         */
        int encodedKeyArraySize = PrefixSharingHelper.SMALLEST_POSSIBLE_COMPRESSION_SIZE;

        if (isLeaf()) {
            //subtract a 64 bit prefix and divide by 8 (the number of bytes in a long)
            maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) >>> 3 ) + 1;
        } else {
            //inner nodes also contain children ids which are ints
            //need to divide by 4
            //n * 4
            maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) >>> 2 ) + 1;
        }

        return maxPossibleNumEntries;
    }

    @Override
    public long getNonKeyEntrySizeInBytes(int numKeys) {
        if (isLeaf()) {
            return numKeys * 8;
        } else {
            return (numKeys + 1) * 4;
        }
    }
}
