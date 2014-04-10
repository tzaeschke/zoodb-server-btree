package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Shared behaviour of unique and non-unique B+ tree.
 */
public abstract class BTree<T extends BTreeNode> {

    private Set<BTreeLeafIterator> iterators =
            Collections.newSetFromMap(new WeakHashMap<BTreeLeafIterator, Boolean>());

    protected int innerNodeOrder;
    protected int leafOrder;
    protected T root;
    protected BTreeNodeFactory nodeFactory;

    public BTree(int innerNodeOrder, int leafOrder, BTreeNodeFactory nodeFactory) {
        this.innerNodeOrder = innerNodeOrder;
        this.leafOrder = leafOrder;
        this.nodeFactory = nodeFactory;
    }

    public BTree(int order, BTreeNodeFactory nodeFactory) {
        this.innerNodeOrder = order;
        this.leafOrder = order;
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
            root = (T) nodeFactory.newNode(isUnique(), leafOrder, true, true);
        }
        Pair<LinkedList<T>, T> result = searchNodeWithHistory(key, value);
        LinkedList<T> ancestorStack = result.getA();
        T leaf = result.getB();

        //notify iterators that this leaf is about to change
        notifyIterators();
        if (leaf.isFull()) {
            //split node
            T rightNode = putAndSplit(leaf, key, value);
            insertInInnerNode(leaf, rightNode.getSmallestKey(), rightNode.getSmallestValue(),  rightNode, ancestorStack);
        } else {
            leaf.put(key, value);
        }
    }

    protected void deleteEntry(long key, long value) {
        Pair<LinkedList<T>,T> pair = searchNodeWithHistory(key, value);
        T leaf = pair.getB();
        LinkedList<T> ancestorStack = pair.getA();

        //notify iterators that this leaf is about to change
        notifyIterators();

        deleteFromLeaf(leaf, key, value);

        if (leaf.isRoot()) {
            return;
        }
        long replacementKey = leaf.getSmallestKey();
        long replacementValue = leaf.getSmallestValue();
        T current = leaf;
        T parent = (ancestorStack.size() == 0) ? null : ancestorStack.pop();
        while (current != null && current.isUnderfull()) {
            //check if can borrow 1 value from the left or right siblings
            T rightSibling = (T) current.rightSibling(parent);
            T leftSibling = (T) current.leftSibling(parent);
            if (leftSibling != null && leftSibling.hasExtraKeys()) {
                redistributeKeysFromLeft(current, leftSibling, parent);
            } else if (rightSibling != null && rightSibling.hasExtraKeys()) {
                redistributeKeysFromRight(current, rightSibling, parent);
            } else {
                //at this point, both left and right sibling have the minimum number of keys
                if (leftSibling!= null) {
                    //merge with left sibling
                    parent = mergeWithLeft(this, current, leftSibling, parent);
                } else {
                    //merge with right sibling
                    parent = mergeWithRight(this, current, rightSibling, parent);
                }
            }
            if (current.containsKeyValue(key, value)) {
                current.replaceEntry(key, value, replacementKey, replacementValue);
            }
            current = parent;
            parent = (ancestorStack.size() == 0 ) ? null : ancestorStack.pop();
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
            T newRoot = (T) nodeFactory.newNode(isUnique(), innerNodeOrder, false, true);
            swapRoot(newRoot);
            root.put(key, value, left, right);
        } else {
            T parent = ancestorStack.pop();
            //check if parent overflows
            if (parent.isFull()) {
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
        while (!current.isLeaf()) {
            current.markChanged();
            stack.push(current);
            current = current.findChild(key, value);
        }
        return new Pair<>(stack, current);
    }

    protected void deleteFromLeaf(T leaf, long key, long value) {
        leaf.delete(key, value);
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

    public int getInnerNodeOrder() {
        return innerNodeOrder;
    }

    public int getLeafOrder() {
        return leafOrder;
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

        if (leafOrder != tree.getLeafOrder()) return false;
        if (innerNodeOrder != tree.getInnerNodeOrder()) return false;
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
        int order = current.getOrder();
        int numKeys = current.getNumKeys();
        T tempNode = (T) nodeFactory.newNode(isUnique(), leafOrder + 1, true, false);
        current.copyFromNodeToNode(0, 0, tempNode, 0, 0, numKeys, order);
        tempNode.setNumKeys(numKeys);
        tempNode.put(newKey, value);

        int keysInLeftNode = (int) Math.ceil((order) / 2.0);
        int keysInRightNode = order - keysInLeftNode;

        // populate left node
        tempNode.copyFromNodeToNode(0, 0, current, 0, 0, keysInLeftNode, keysInLeftNode + 1);
        current.setNumKeys(keysInLeftNode);

        // populate right node
        T right = (T) nodeFactory.newNode(isUnique(), leafOrder, true, false);
        tempNode.copyFromNodeToNode(keysInLeftNode, keysInLeftNode, right,
                0, 0, keysInRightNode, keysInRightNode + 1);
        right.setNumKeys(keysInRightNode);
        tempNode.close();

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
        int order = current.getOrder();
        int numKeys = current.getNumKeys();
        // create a temporary node to allow the insertion
        T tempNode = (T) nodeFactory.newNode(isUnique(), innerNodeOrder + 1, false, true);
        current.copyFromNodeToNode(0, 0, tempNode, 0, 0, numKeys, order);
        tempNode.setNumKeys(numKeys);
        tempNode.put(key, value, newNode);

        // split
        T right = (T) nodeFactory.newNode(isUnique(), innerNodeOrder, false, false);
        int keysInLeftNode = (int) Math.floor(order / 2.0);
        // populate left node
        tempNode.copyFromNodeToNode(0, 0, current, 0, 0, keysInLeftNode, keysInLeftNode + 1);
        current.setNumKeys(keysInLeftNode);

        // populate right node
        int keysInRightNode = order - keysInLeftNode - 1;
        tempNode.copyFromNodeToNode(keysInLeftNode + 1, keysInLeftNode + 1, right,
                0, 0, keysInRightNode, keysInRightNode + 1);
        right.setNumKeys(keysInRightNode);
        tempNode.close();

        return new Pair<>(right, tempNode.getKeyValue(keysInLeftNode));
    }


    public T mergeWithRight(BTree<T> tree, T current, T right, T parent) {
        int keyIndex = parent.keyIndexOf(current, right);

        //check if parent needs merging -> tree gets smaller
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            if (parent.getKey(0) != right.getKey(0)) {
                right.shiftRecordsRight(parent.getNumKeys());
                right.migrateEntry(0, parent, 0);
                right.increaseNumKeys(1);
            }
            right.shiftRecordsRight(current.getNumKeys());
            copyMergeFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
            right.increaseNumKeys(current.getNumKeys());
            tree.swapRoot(right);
            parent.close();
            right.changeOrder(leafOrder);
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

        //this node will not be used anymore
        current.close();
        return parent;
    }

    public T  mergeWithLeft(BTree<T> tree, T current, T left, T parent) {
        int keyIndex = parent.keyIndexOf(left, current);

        //check if we need to merge with parent
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            current.shiftRecordsRight(parent.getNumKeys());
            current.migrateEntry(0, parent, 0);
            current.increaseNumKeys(parent.getNumKeys());

            current.shiftRecordsRight(left.getNumKeys());
            copyNodeToAnother(left, current, 0);
            current.increaseNumKeys(left.getNumKeys());
            tree.swapRoot(current);
            parent.close();
            current.changeOrder(leafOrder);
            parent = current;
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

        //left wont be used anymore
        left.close();
        return parent;
    }

    public void redistributeKeysFromRight(T current, T right, T parent) {
        int totalKeys = right.getNumKeys() + current.getNumKeys();
        int keysToMove = right.getNumKeys() - (totalKeys / 2);

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
    }

    public void redistributeKeysFromLeft(T current, T left, T parent) {
        int totalKeys = left.getNumKeys() + current.getNumKeys();
        int keysToMove = left.getNumKeys() - (totalKeys / 2);
        int parentKeyIndex = parent.keyIndexOf(left, current);
        if (current.isLeaf()) {
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
    }

    public void copyMergeFromLeftNodeToRightNode(T src, int srcStart, T dest, int destStart, int keys, int children) {
        src.copyFromNodeToNode(srcStart, srcStart, dest, destStart, destStart, keys, children + 1);
    }

    public void copyRedistributeFromLeftNodeToRightNode(T src, int srcStart, T dest, int destStart,
                                                        int keys, int children) {
        src.copyFromNodeToNode( srcStart, srcStart + 1, dest, destStart, destStart, keys, children);
    }

    private void copyFromRightNodeToLeftNode(T src,  int srcStart, T dest, int destStart,
                                             int keys, int children) {
        src.copyFromNodeToNode(srcStart, srcStart, dest, destStart, destStart, keys, children);
    }

    private void copyNodeToAnother(T source, T destination, int destinationIndex) {
        source.copyFromNodeToNode(0, 0, destination, destinationIndex, destinationIndex, source.getNumKeys(), source.getNumKeys() + 1);
    }

    public void registerIterator(BTreeLeafIterator iterator) {
        iterators.add(iterator);
    }

    public void deregisterIterator(BTreeLeafIterator iterator) {
        iterators.add(iterator);
    }

    private void notifyIterators() {
        for (BTreeLeafIterator iterator : iterators) {
            iterator.handleNodeChange();
        }
    }
}
