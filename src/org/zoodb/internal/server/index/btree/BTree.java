package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;
import org.zoodb.internal.util.Pair;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Shared behaviour of unique and non-unique B+ tree.
 */
public abstract class BTree<T extends BTreeNode> {

    protected T root;
    protected BTreeNodeFactory nodeFactory;
    protected int pageSize;
    private long maxKey = Long.MIN_VALUE;
    private long minKey = Long.MIN_VALUE;
    
    private int modcount = 0; // number of modifications of the tree

    public BTree(BTreeNodeFactory nodeFactory) {
        this.nodeFactory = nodeFactory;
    }

    public BTree(int pageSize, BTreeNodeFactory nodeFactory) {
        this.pageSize = pageSize;
        this.nodeFactory = nodeFactory;
    }

    public abstract boolean isUnique();

    /**
     * Insert a new key value pair to the B+ tree.
     *
     * Algorithm performs as follows:
     *  - a reference to the leaf node on which the value for the key received as argument is first retrieved
     *  - if the leaf will not overflow after adding the new (key, value) pair, the pair is inserted in the leaf.
     *  - if the leaf will overflow after the addition of the new (key, value) pair, the leaf is split into 2 leaves.
     *    The keys/values in the original leaf are split as evenly as possible between the 2 leaves.
     *    The references to the parent node are then fixed.
     * @param key               The new key to be inserted
     * @param value         The new value to be inserted
     */
    public void insert(long key, long value) {
        if (root == null) {
            root = (T) nodeFactory.newNode(isUnique(), pageSize, true, true);
        }
        Pair<LinkedList<T>, T> result = searchNodeWithHistory(key, value);
        LinkedList<T> ancestorStack = result.getA();
        T leaf = result.getB();

        increaseModcount();
        if (leaf.willOverflowAfterInsert(key, value)) {
//            T parent = ancestorStack.peek();
//            T leftSibling = (T) leaf.leftSibling(parent);
//            if (leftSibling != null && !leftSibling.willOverflowAfterInsert(leaf.getSmallestKey())) {
//                redistributeKeysFromRight(leftSibling, leaf, parent);
//            } else {
                T rightNode = putAndSplit(leaf, key, value);
                insertInInnerNode(leaf, rightNode.getSmallestKey(), rightNode.getSmallestValue(),  rightNode, ancestorStack);
            //}
        } else {
            leaf.put(key, value);
        }
        
        maxKey = Math.max(maxKey, key);
        minKey = Math.min(minKey, key);
    }

	protected long deleteEntry(long key, long value) {
		if(root.getNumKeys() == 0) {
			throw new NoSuchElementException();
		}
        Pair<LinkedList<T>,T> pair = searchNodeWithHistory(key, value);
        T leaf = pair.getB();
        LinkedList<T> ancestorStack = pair.getA();

        increaseModcount();

        T leafParent = ancestorStack.peek();
        long oldValue = deleteFromLeaf(leaf, key, value);
        //ToDo should this happen?
        if (!leaf.isRoot() && leaf.getNumKeys() == 0) {
            leafParent.removeChild(leaf);
            System.out.println("Remove child leaf");
            leaf.close();
            if (ancestorStack.size() > 1) {
                leaf = leafParent;
                ancestorStack.pop();
            }
        }
        rebalanceAfterDelete(leaf, ancestorStack, key, value);
        if(key == minKey) {
        	minKey = computeMinKey();
        }
        if(key == maxKey) {
        	maxKey = computeMaxKey();
        }
        return oldValue;
    }

    private void rebalanceAfterDelete(T node, LinkedList<T> ancestorStack, long key, long value) {
        if (!node.isRoot()) {
            T current = node;
            T parent = (ancestorStack.size() == 0) ? null : ancestorStack.pop();

            while (current != null) {
//                if (current.getNumKeys() == 0) {
//                    //System.out.println("Remove child inner");
//                    //parent.removeChild(current);
//                    current.close();
//                } else
                if (current.isUnderfull()) {
                    //check if can borrow 1 value from the left or right siblings
                    T rightSibling = (T) current.rightSibling(parent);
                    T leftSibling = (T) current.leftSibling(parent);
                    if (current.fitsIntoOneNodeWith(leftSibling)) {
                        //System.out.println("Merge with left");
                        //merge with left sibling
                        parent = mergeWithLeft(this, current, leftSibling, parent);
                    } else if (current.fitsIntoOneNodeWith(rightSibling)) {
                        //System.out.println("Merge with left");
                        parent = mergeWithRight(this, current, rightSibling, parent);
                    } else if (leftSibling != null && leftSibling.hasExtraKeys()) {
                        //System.out.println("Redistribute from left");
                        redistributeKeysFromLeft(current, leftSibling, parent);
                    } else if (rightSibling != null && rightSibling.hasExtraKeys()) {
                        //System.out.println("Redistribute from right");
                        redistributeKeysFromRight(current, rightSibling, parent);
                    } else {
                        //System.out.println("Could not re-balance");
                    }
                    //if nothing works, we're left with an under-full node, but
                    //there's nothing really that we can do at this point
                }
                current = parent;
                parent = (ancestorStack.size() == 0 ) ? null : ancestorStack.pop();
            }
        }
    }
    /**
     * Inserts the nodes left and right into the parent node. The right node is a new node, created
     * as a result of a split.
     *
     * @param left              The left node.
     * @param key               The key that should separate them in the parent.
     * @param right             The right node.
     * @param ancestorStack     The ancestor stack that should be traversed.
     */
    private void insertInInnerNode(T left, long key, long value, T right, LinkedList<T> ancestorStack) {
        if (left.isRoot()) {
            T newRoot = (T) nodeFactory.newNode(isUnique(), getPageSize(), false, true);
            swapRoot(newRoot);
            root.put(key, value, left, right);
        } else {
            T parent = ancestorStack.pop();
            //check if parent overflows
            if (parent.willOverflowAfterInsert(key, value)) {
                Pair<T, Pair<Long, Long> > pair = putAndSplit(parent, key, value, right);
                T newNode = pair.getA();
                Pair<Long, Long> keyValuePair = pair.getB();
                long keyToMoveUp = keyValuePair.getA();
                long valueToMoveUp = keyValuePair.getB();
                insertInInnerNode(parent, keyToMoveUp, valueToMoveUp, newNode, ancestorStack);
            } else {
                parent.put(key, value, right);
            }
        }
    }

    protected Pair<LinkedList<T>, T> searchNodeWithHistory(long key, long value) {
        LinkedList<T> stack = new LinkedList<>();
        T current = root;
        
        if(root == null) {
        	throw new NoSuchElementException();
        }
        while (!current.isLeaf()) {
            current.markChanged();
            stack.push(current);
            current = current.findChild(key, value);
        }
        return new Pair<>(stack, current);
    }

    protected long deleteFromLeaf(T leaf, long key, long value) {
        long oldValue = leaf.delete(key, value);
        return oldValue;
    }

    public void setRoot(BTreeNode root) {
        this.root = (T) root;
    }

    public boolean isEmpty() {
        return root==null;
    }

    public T getRoot() {
        return this.root;
    }

    public String toString() {
        if(this.root != null) {
            return this.root.toString();
        } else {
            return "Empty tree";
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BTree)) return false;

        BTree tree = (BTree) o;

        if (pageSize != tree.getPageSize()) return false;
        if (root != null ? !root.equals(tree.getRoot()) : tree.getRoot() != null) return false;

        return true;
    }

    public void swapRoot(T newRoot) {
        if (root != null) {
            root.setIsRoot(false);
        }
        root = newRoot;
        if (newRoot != null) {
            newRoot.setIsRoot(true);
        }
    }

    public BTreeNodeFactory getNodeFactory() {
        return nodeFactory;
    }
    
    /*
     * Counts number of nodes in the tree.
     * WARNING: SLOW! has to iterate over whole tree
     */
    public int size() {
    	BTreeIterator it = new BTreeIterator(this);
    	int counter = 0;
    	while(it.hasNext()) {
    		it.next();
    		counter++;
    	}

    	return counter;
    }

    /**
     * Puts a new key into the node and splits accordingly. Returns the newly
     * created leaf, which is to the right.
     *
     * @param newKey
     * @return
     */
    public <T extends BTreeNode> T putAndSplit(T current, long newKey, long value) {
        if (!current.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on leaf nodes.");
        }
        int pageSize = current.getPageSize();
        int numKeys = current.getNumKeys();
        T tempNode = (T) nodeFactory.newNode(isUnique(), getPageSize(), true, false);
        int childrenArraySize = current.getNumKeys() + 1;
        current.copyFromNodeToNode(0, 0, tempNode, 0, 0, numKeys, childrenArraySize);
        tempNode.setNumKeys(numKeys);
        tempNode.put(newKey, value);

        //ToDo move the computation of the index on the paged node
        int weightKey = 8;
        int weightChild = 0;
        int header = tempNode.storageHeaderSize();
        int keysInLeftNode = PrefixSharingHelper.computeIndexForSplitAfterInsert(
                tempNode.getKeys(), tempNode.getNumKeys(),
                header, weightKey, weightChild, getPageSize());

        int keysInRightNode = numKeys + 1 - keysInLeftNode;

        // populate left node
        tempNode.copyFromNodeToNode(0, 0, current, 0, 0, keysInLeftNode, keysInLeftNode + 1);
        current.setNumKeys(keysInLeftNode);

        // populate right node
        T right = (T) nodeFactory.newNode(isUnique(), pageSize, true, false);
        tempNode.copyFromNodeToNode(keysInLeftNode, keysInLeftNode, right,
                0, 0, keysInRightNode, keysInRightNode + 1);
        right.setNumKeys(keysInRightNode);
        tempNode.close();

        assert current.computeSize() <= current.getPageSize();
        assert right.computeSize() <= right.getPageSize();

        return right;
    }

    /**
     * Puts a key and a new node to the inner structure of the tree.
     *
     * @param key
     * @param newNode
     * @return
     */
    public <T extends BTreeNode> Pair<T, Pair<Long, Long> > putAndSplit(T current, long key, long value, T newNode) {
        if (current.isLeaf()) {
            throw new IllegalStateException(
                    "Should only be called on inner nodes.");
        }
        int pageSize = current.getPageSize();
        int numKeys = current.getNumKeys();
        // create a temporary node to allow the insertion
        T tempNode = (T) nodeFactory.newNode(isUnique(), pageSize, false, true);
        int childrenArraySize = current.getNumKeys() + 1;
        current.copyFromNodeToNode(0, 0, tempNode, 0, 0, numKeys, childrenArraySize);
        tempNode.setNumKeys(numKeys);
        tempNode.put(key, value, newNode);

        int weightKey = (isUnique() ? 8 : 0);
        int weightChild = 4;
        int header = tempNode.storageHeaderSize();
        // split
        T right = (T) nodeFactory.newNode(isUnique(), pageSize, false, false);
        int keysInLeftNode = PrefixSharingHelper.computeIndexForSplitAfterInsert(tempNode.getKeys(), tempNode.getNumKeys(), header, weightKey, weightChild, getPageSize());
        int keysInRightNode = numKeys - keysInLeftNode;

        // populate left node
        tempNode.copyFromNodeToNode(0, 0, current, 0, 0, keysInLeftNode, keysInLeftNode + 1);
        current.setNumKeys(keysInLeftNode);

        // populate right node
        tempNode.copyFromNodeToNode(keysInLeftNode + 1, keysInLeftNode + 1, right,
                0, 0, keysInRightNode, keysInRightNode + 1);
        right.setNumKeys(keysInRightNode);
        tempNode.close();

        assert current.computeSize() <= current.getPageSize();
        assert right.computeSize() <= right.getPageSize();

        return new Pair<>(right, tempNode.getKeyValue(keysInLeftNode));
    }

    public T mergeWithRight(BTree<T> tree, T current, T right, T parent) {
        int keyIndex = parent.keyIndexOf(current, right);

        //check if parent needs merging -> tree gets smaller
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            if (!current.isLeaf()) {
                right.shiftRecordsRight(parent.getNumKeys());
                right.migrateEntry(0, parent, 0);
                right.increaseNumKeys(1);
            }
            right.shiftRecordsRight(current.getNumKeys());
            copyMergeFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
            right.increaseNumKeys(current.getNumKeys());
            tree.swapRoot(right);
            parent.close();
            parent = right;
        } else {
            if (right.isLeaf()) {
                //merge leaves
                parent.shiftRecordsLeftWithIndex(keyIndex, 1);
                parent.decreaseNumKeys(1);
                right.shiftRecordsRight(current.getNumKeys());
                copyMergeFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
                right.increaseNumKeys(current.getNumKeys());
            } else {
                //merge inner nodes
                right.shiftRecordsRight(1);
                right.migrateEntry(0, parent, keyIndex);
                right.increaseNumKeys(1);
                parent.shiftRecordsLeftWithIndex(keyIndex, 1);
                parent.decreaseNumKeys(1);

                right.shiftRecordsRight(current.getNumKeys());
                copyMergeFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
                right.increaseNumKeys(current.getNumKeys());
            }
        }
        if (current.getNumKeys() == 0 || parent.getNumKeys() == 0 || right.getNumKeys() == 0) {
            System.out.println("Failure during redistribution");
        }
        //this node will not be used anymore
        current.close();

        assert right.computeSize() <= right.getPageSize();
        assert parent.computeSize() <= parent.getPageSize();

        return parent;
    }

    public T  mergeWithLeft(BTree<T> tree, T current, T left, T parent) {
        int keyIndex = parent.keyIndexOf(left, current);

        //check if we need to merge with parent
        if (parent.getNumKeys() == 1) {
            if (parent.isRoot()) {
                if (!current.isLeaf()) {
                    current.shiftRecordsRight(parent.getNumKeys());
                    current.migrateEntry(0, parent, 0);
                    current.increaseNumKeys(parent.getNumKeys());
                }

                current.shiftRecordsRight(left.getNumKeys());
                copyNodeToAnother(left, current, 0);
                current.increaseNumKeys(left.getNumKeys());
                tree.swapRoot(current);
                parent.close();
                parent = current;
            } else {
                return parent;
            }
        } else {
            if (current.isLeaf()) {
                //leaf node merge
                parent.shiftRecordsLeftWithIndex(keyIndex, 1);
                parent.decreaseNumKeys(1);

                current.shiftRecordsRight(left.getNumKeys());
                copyNodeToAnother(left, current, 0);
                current.increaseNumKeys(left.getNumKeys());
            } else {
                //inner node merge
                //move key from parent
                current.shiftRecordsRight(left.getNumKeys() + 1);
                current.migrateEntry(left.getNumKeys(), parent, keyIndex);
                parent.shiftRecordsLeftWithIndex(keyIndex, 1);
                parent.decreaseNumKeys(1);

                //copy from left node
                copyNodeToAnother(left, current, 0);
                current.increaseNumKeys(left.getNumKeys() + 1);
            }
        }
        if (current.getNumKeys() == 0 || parent.getNumKeys() == 0 || left.getNumKeys() == 0) {
            System.out.println("Failure during redistribution");
        }
        //left wont be used anymore
        left.close();

        assert current.computeSize() <= current.getPageSize();
        assert parent.computeSize() <= parent.getPageSize();

        return parent;
    }

    public void redistributeKeysFromRight(T current, T right, T parent) {
        int weightKey = (current.isLeaf() || (!isUnique())) ? 8 : 0;
        int weightChild = (current.isLeaf() ? 0 : 4);
        int header = current.storageHeaderSize();

        int splitIndexInRight = PrefixSharingHelper.computeIndexForRedistributeRightToLeft(
                current.getKeys(), current.getNumKeys(), right.getKeys(), right.getNumKeys(), header, weightKey, weightChild, current.getPageSize());
        //int keysToMove = right.getNumKeys() - splitIndexInRight;
        int keysToMove = splitIndexInRight + 1;
        if (keysToMove == 0) {
            return;
        }
        //move key from parent to current node
        int parentKeyIndex = parent.keyIndexOf(current, right);
        if (current.isLeaf()) {
            int startIndexRight = 0;
            int startIndexLeft = current.getNumKeys();
            //copy from left to current
            copyFromRightNodeToLeftNode(right, startIndexRight, current, startIndexLeft, keysToMove, keysToMove);

            //shift nodes in current node right
            right.shiftRecordsLeft(keysToMove);
            //fix number of keys
            right.decreaseNumKeys(keysToMove);
            current.increaseNumKeys(keysToMove);

            parent.migrateEntry(parentKeyIndex, right, 0);
        } else {
            //add key from parent
            current.migrateEntry(current.getNumKeys(), parent, parentKeyIndex);
            current.increaseNumKeys(1);

            int startIndexRight = 0;
            int startIndexLeft = current.getNumKeys();
            keysToMove--;
            //copy from left to current
            copyFromRightNodeToLeftNode(right, startIndexRight, current, startIndexLeft, keysToMove, keysToMove + 1);
            current.increaseNumKeys(keysToMove);

            //shift nodes in current node right
            right.shiftRecordsLeft(keysToMove);
            right.decreaseNumKeys(keysToMove);
            parent.migrateEntry(parentKeyIndex, right, 0);
            right.shiftRecordsLeft(1);
            right.decreaseNumKeys(1);
        }

        assert current.computeSize() <= current.getPageSize();
        assert right.computeSize() <= right.getPageSize();
        assert parent.computeSize() <= parent.getPageSize();

    }

    public void redistributeKeysFromLeft(T current, T left, T parent) {
        //int totalKeys = left.getNumKeys() + current.getNumKeys();
        int weightKey = (current.isLeaf() || (!isUnique())) ? 8 : 0;
        int weightChild = (current.isLeaf() ? 0 : 4);
        int header = current.storageHeaderSize();
        int splitIndexInLeft = PrefixSharingHelper.computeIndexForRedistributeLeftToRight(
                left.getKeys(), left.getNumKeys(), current.getKeys(), current.getNumKeys(),
                header, weightKey, weightChild, current.getPageSize());
        int keysToMove = left.getNumKeys() - splitIndexInLeft;
        //int keysToMove = left.getNumKeys() - (totalKeys / 2);
        int parentKeyIndex = parent.keyIndexOf(left, current);
        if (current.isLeaf()) {
            if (keysToMove == 0) {
                return;
            }
            //shift nodes in current node right
            current.shiftRecordsRight(keysToMove);

            int startIndexLeft = left.getNumKeys() - keysToMove;
            int startIndexRight = 0;
            //copy from left to current
            copyRedistributeFromLeftNodeToRightNode(left, startIndexLeft, current, startIndexRight, keysToMove, keysToMove);

            //fix number of keys
            left.decreaseNumKeys(keysToMove);
            current.increaseNumKeys(keysToMove);

            //move key from parent to current node
            parent.migrateEntry(parentKeyIndex, current, 0);
        } else {
            keysToMove-=1;
            if (current.getNumKeys() == 0) {
                keysToMove--;
            }
            if (keysToMove == 0) {
                return;
            }
            int startIndexLeft = left.getNumKeys() - keysToMove;
            int startIndexRight = 0;

            current.shiftRecordsRight(1);
            current.increaseNumKeys(1);
            current.migrateEntry(0, parent, parentKeyIndex);
            //shift nodes in current node right
            current.shiftRecordsRight(keysToMove);

            //copy k keys and k+1 children from left
            left.copyFromNodeToNode(startIndexLeft, startIndexLeft, current, startIndexRight, startIndexRight, keysToMove, keysToMove + 1);
            current.increaseNumKeys(keysToMove);
            left.decreaseNumKeys(keysToMove);
            //move the biggest key to parent
            parent.migrateEntry(parentKeyIndex, left, left.getNumKeys() - 1);
            left.decreaseNumKeys(1);
        }

        assert current.computeSize() <= current.getPageSize();
        assert left.computeSize() <= left.getPageSize();
        assert parent.computeSize() <= parent.getPageSize();

    }

    public void copyMergeFromLeftNodeToRightNode(T src, int srcStart, T dest, int destStart, int keys, int children) {
        src.copyFromNodeToNode(srcStart, srcStart, dest, destStart, destStart, keys, children + 1);
    }

    public void copyRedistributeFromLeftNodeToRightNode(T src, int srcStart, T dest, int destStart,
                                                        int keys, int children) {
        src.copyFromNodeToNode( srcStart, srcStart + 1, dest, destStart, destStart, keys, children);
    }
    
    public long getMaxKey() {
		return maxKey;
	}

	public long getMinKey() {
		return minKey;
	}

	public long computeMinKey() {
        if (getRoot().getNumKeys() == 0) {
            return -1;
        }
        BTreeLeafEntryIterator<T> it = new AscendingBTreeLeafEntryIterator<T>(this);
        long minKey = Long.MIN_VALUE;
        if(it.hasNext()) {
                minKey = it.next().getKey();
        }
        return minKey;
    }

    public long computeMaxKey() {
        //ToDo cleanup code
        if (getRoot().getNumKeys() == 0) {
            return Long.MIN_VALUE;
        }
        BTreeLeafEntryIterator<T> it = new DescendingBTreeLeafEntryIterator<T>(this);
        long maxKey = Long.MIN_VALUE;
        if(it.hasNext()) {
                maxKey = it.next().getKey();
        }
        return maxKey;
    }
    
    public int statsGetInnerN() {
    	BTreeIterator it = new BTreeIterator(this);
		int innerN = 0;
		while(it.hasNext()) {
			if(!it.next().isLeaf()) innerN++;
		}
		return innerN;
    }
    
    public int statsGetLeavesN() {
    	BTreeIterator it = new BTreeIterator(this);
		int leafN = 0;
		while(it.hasNext()) {
			if(it.next().isLeaf()) leafN++;
		}
		return leafN;
    }
    
    private void copyFromRightNodeToLeftNode(T src,  int srcStart, T dest, int destStart,
                                             int keys, int children) {
        src.copyFromNodeToNode(srcStart, srcStart, dest, destStart, destStart, keys, children);
    }

    private void copyNodeToAnother(T source, T destination, int destinationIndex) {
        source.copyFromNodeToNode(0, 0, destination, destinationIndex, destinationIndex, source.getNumKeys(), source.getNumKeys() + 1);
    }

    private void increaseModcount() {
    	modcount++;
	}
    
    public int getModcount() {
    	return this.modcount;
    }

    public int getPageSize() {
        return pageSize;
    }

    private long[] insertedOrderedInArray(long newKey, long[] keys, int size) {
        int index = Arrays.binarySearch(keys, 0, size, newKey);
        if (index < 0) {
            index = - (index + 1);
        }
        System.arraycopy(keys, index, keys, index + 1, size);
        keys[index] = newKey;
        return keys;
    }
}
