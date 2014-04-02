package org.zoodb.internal.server.index.btree;

public class BTreeUtils {
    //Todo Move to an abstract node class

    public static <T extends BTreeNode> T mergeWithRight(BTree<T> tree, T current, T right, T parent) {
        int keyIndex = parent.keyIndexOf(current, right);

        //check if parent needs merging -> tree gets smaller
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            if (parent.getKey(0) != right.getKey(0)) {
                right.shiftRecordsRight(parent.getNumKeys());
                right.setKey(0, parent.getKey(0));
                right.increaseNumKeys(1);
            }
            right.shiftRecordsRight(current.getNumKeys());
            copyMergeFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
            right.increaseNumKeys(current.getNumKeys());
            tree.swapRoot(right);
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
                right.setKey(0, parent.getKey(keyIndex));
                right.increaseNumKeys(1);
                parent.shiftRecordsLeftWithIndex(keyIndex, 1);
                parent.decreaseNumKeys(1);

                right.shiftRecordsRight(current.getNumKeys());
                copyMergeFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
                right.increaseNumKeys(current.getNumKeys());
            }
        }
        return parent;
    }

    public static <T extends BTreeNode> T  mergeWithLeft(BTree<T> tree, T current, T left, T parent) {
        int keyIndex = parent.keyIndexOf(left, current);

        //check if we need to merge with parent
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            current.shiftRecordsRight(parent.getNumKeys());
            current.setKey(0, parent.getKey(0));
            current.increaseNumKeys(parent.getNumKeys());

            current.shiftRecordsRight(left.getNumKeys());
            copyNodeToAnother(left, current, 0);
            current.increaseNumKeys(left.getNumKeys());
            tree.swapRoot(current);
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
                current.setKey(left.getNumKeys(), parent.getKey(keyIndex));
                parent.shiftRecordsLeftWithIndex(keyIndex, 1);
                parent.decreaseNumKeys(1);

                //copy from left node
                copyNodeToAnother(left, current, 0);
                current.increaseNumKeys(left.getNumKeys() + 1);
            }

        }


        return parent;
    }

    public static <T extends BTreeNode> void redistributeKeysFromRight(T current, T right, T parent) {
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

            parent.setKey(parentKeyIndex, right.getSmallestKey());
        } else {
            //add key from parent
            current.setKey(current.getNumKeys(), parent.getKey(parentKeyIndex));
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
            parent.setKey(parentKeyIndex, right.getSmallestKey());
            right.shiftRecordsLeft(1);
            right.decreaseNumKeys(1);

        }
    }

    public static <T extends BTreeNode> void redistributeKeysFromLeft(T current, T left, T parent) {
        int totalKeys = left.getNumKeys() + current.getNumKeys();
        int keysToMove = left.getNumKeys() - (totalKeys / 2);
        int parentKeyIndex = parent.keyIndexOf(left, current);
        if (current.isLeaf()) {
            //shift nodes in current node right
            current.shiftRecordsRight(keysToMove);

            //int startIndexLeft = (totalKeys + 1) / 2;
            int startIndexLeft = left.getNumKeys() - keysToMove;
            int startIndexRight = 0;
            //copy from left to current
            copyRedistributeFromLeftNodeToRightNode(left, startIndexLeft, current, startIndexRight, keysToMove, keysToMove);

            //fix number of keys
            left.decreaseNumKeys(keysToMove);
            current.increaseNumKeys(keysToMove);

            //move key from parent to current node
            parent.setKey(parentKeyIndex, current.getSmallestKey());
        } else {
            keysToMove-=1;
            if (current.getNumKeys() == 0) {
                keysToMove--;
            }
            int startIndexLeft = left.getNumKeys() - keysToMove;
            int startIndexRight = 0;

            current.shiftRecordsRight(1);
            current.increaseNumKeys(1);
            current.setKey(0, parent.getKey(parentKeyIndex));

            //shift nodes in current node right
            current.shiftRecordsRight(keysToMove);

            //copy k keys and k+1 children from left
            left.copyFromNodeToNode(startIndexLeft, startIndexLeft, current, startIndexRight, startIndexRight, keysToMove, keysToMove + 1);
            current.increaseNumKeys(keysToMove);
            left.decreaseNumKeys(keysToMove);
            //move the biggest key to parent
            parent.setKey(parentKeyIndex, left.getLargestKey());

            left.decreaseNumKeys(1);
        }
    }

    public static <T extends BTreeNode> void copyMergeFromLeftNodeToRightNode(T source,
                                                               int sourceStartIndex,
                                                               T destination,
                                                               int destinationStartIndex,
                                                               int keys,
                                                               int children) {
        source.copyFromNodeToNode(
                sourceStartIndex,
                sourceStartIndex,
                destination,
                destinationStartIndex,
                destinationStartIndex,
                keys,
                children + 1);
    }

    public static <T extends BTreeNode> void copyRedistributeFromLeftNodeToRightNode(T source,
                                                   int sourceStartIndex,
                                                   T destination,
                                                   int destinationStartIndex,
                                                   int keys,
                                                   int children) {
        source.copyFromNodeToNode(
                sourceStartIndex,
                sourceStartIndex + 1,
                destination,
                destinationStartIndex,
                destinationStartIndex,
                keys,
                children);
    }

    public static <T extends BTreeNode> void copyFromRightNodeToLeftNode(T source,
                                                   int sourceStartIndex,
                                                   T destination,
                                                   int destinationStartIndex,
                                                   int keys,
                                                   int children) {
        source.copyFromNodeToNode(
                sourceStartIndex,
                sourceStartIndex,
                destination,
                destinationStartIndex,
                destinationStartIndex,
                keys,
                children);
    }

    public static <T extends BTreeNode> void copyNodeToAnother(T source, T destination, int destinationIndex) {
        source.copyFromNodeToNode(0, 0, destination, destinationIndex, destinationIndex, source.getNumKeys(), source.getNumKeys() + 1);
    }

    /**
     * Root-node put.
     *
     * Used when a non-leaf root is empty and will be populated by a single key
     * and two nodes.
     *
     * @param key
     *            The new key on the root.
     * @param left
     *            The left node.
     * @param right
     *            The right node.
     */
    public static <T extends BTreeNode> void put(T root, long key, T left, T right) {
        if (!root.isRoot()) {
            throw new IllegalStateException(
                    "Should only be called on the root node.");
        }
        root.setKey(0, key);
        root.setNumKeys(1);

        root.setChild(0, left);
        root.setChild(1, right);
    }
}
