package org.zoodb.internal.server.index.btree.nonunique;

import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.PagedBTreeNode;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

import java.util.Arrays;

public class NonUniquePagedBTreeNode extends PagedBTreeNode {

    public NonUniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot) {
        super(bufferManager, order, isLeaf, isRoot);
    }

    public NonUniquePagedBTreeNode(BTreeBufferManager bufferManager, int order, boolean isLeaf, boolean isRoot, int pageId) {
        super(bufferManager, order, isLeaf, isRoot, pageId);
    }

    @Override
    protected void copyValues(PagedBTreeNode node) {
        node.setValues(Arrays.copyOf(node.getValues(), node.getValues().length));
    }

    @Override
    public void initializeEntries() {
        int size = computeMaxPossibleNumEntries();
        initKeys(size);
        initValues(size);
        if (!isLeaf()) {
            initChildren(size + 1);
        }
    }

    @Override
    public BTreeNode newNode(int order, boolean isLeaf, boolean isRoot) {
        return new NonUniquePagedBTreeNode(bufferManager, order, isLeaf, isRoot);
    }

    @Override
    public void migrateEntry(int destinationPos, BTreeNode source, int sourcePos) {
        long key = source.getKey(sourcePos);
        long value = source.getValue(sourcePos);
        setKey(destinationPos, key);
        setValue(destinationPos, value);

        markDirty();
    }

    @Override
    public void setEntry(int pos, long key, long value) {
        setKey(pos, key);
        setValue(pos, value);
    }

    @Override
    public void copyFromNodeToNode(int srcStartK, int srcStartC, BTreeNode destination, int destStartK, int destStartC, int keys, int children) {
        BTreeNode source = this;
        System.arraycopy(source.getKeys(), srcStartK, destination.getKeys(), destStartK, keys);
        System.arraycopy(source.getValues(), srcStartK, destination.getValues(), destStartK, keys);
        if (!destination.isLeaf()) {
            source.copyChildren(source, srcStartC, destination, destStartC, children);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecords(int startIndex, int endIndex, int amount) {
        shiftKeys(startIndex, endIndex, amount);
        shiftValues(startIndex, endIndex, amount);
        if (!isLeaf()) {
            shiftChildren(startIndex, endIndex, amount + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsRight(int amount) {
        shiftKeys(0, amount, getNumKeys());
        shiftValues(0, amount, getNumKeys());
        if (!isLeaf()) {
            shiftChildren(0, amount, getNumKeys() + 1);
        }
        
        markDirty();
    }

    @Override
    public void shiftRecordsLeftWithIndex(int startIndex, int amount) {
        int keysToMove = getNumKeys() - amount - startIndex;
        shiftKeys(startIndex + amount, startIndex, keysToMove);
        shiftValues(startIndex + amount, startIndex, keysToMove);
        if (!isLeaf()) {
            shiftChildren(startIndex + amount, startIndex, keysToMove + 1);
        }
        
        markDirty();
    }

    @Override
    protected boolean containsAtPosition(int position, long key, long value) {
        return this.getKey(position) == key && getValue(position) == value;
    }

    @Override
    protected boolean smallerThanKeyValue(int position, long key, long value) {
        return (key < getKey(position) ||
                (key == getKey(position) && value < getValue(position)));
    }
    
    @Override
    protected boolean allowNonUniqueKeys() {
    	return true;
    }

    @Override
    public long getNonKeyEntrySizeInBytes(int numKeys) {
        if (isLeaf()) {
            return numKeys << 3;
        } else {
            int numChildren = numKeys + 1;
            return (numKeys << 3) + (numChildren << 2);
        }
    }

    @Override
    public int computeMaxPossibleNumEntries() {
        int maxPossibleNumEntries;
        /*
            In the case of the best compression, all keys would have the same value.
         */
        int encodedKeyArraySize = PrefixSharingHelper.SMALLEST_POSSIBLE_COMPRESSION_SIZE;
        if (pageSize < encodedKeyArraySize) {
            throw new IllegalStateException("The page size is too small!");
        }
        if (isLeaf()) {
            //subtract a 64 bit prefix and divide by 8 (the number of bytes in a long)

            maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) >>> 3 ) + 1;
        } else {
            //inner nodes also contain children ids which are ints
            //need to divide by 12
            // n * 8 value bytes
            // (n + 1) * 4 child bytes
            maxPossibleNumEntries = ((pageSize - encodedKeyArraySize) / 12 ) + 1;
        }
        return maxPossibleNumEntries;
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
        ret += ",   \tv:";
        ret += "[";
        for (int i = 0; i < this.getNumKeys(); i++) {
            ret += Long.toString(getValue(i));
            if (i != this.getNumKeys() - 1)
                ret += " ";
        }
        ret += "]";

        if (!isLeaf()) {
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
