package org.zoodb.internal.server.index.btree;

public class BTreeUtils {

    public static void mergeWithRight(BTree tree, BTreeNode current, BTreeNode right) {
        BTreeNode parent = right.getParent();
        int keyIndex = parent.keyIndexOf(current, right);
        if (parent.isRoot() && parent.getNumKeys() == 1) {
            right.shiftRecordsRight(parent.getNumKeys());
            current.setKey(0, parent.getKey(0));
            right.increaseNumKeys(parent.getNumKeys());

            right.shiftRecordsRight(current.getNumKeys());
            BTreeUtils.copyFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
            right.increaseNumKeys(current.getNumKeys());
            tree.setRoot(right);
            right.setParent(null);
        } else {
            parent.shiftRecordsLeftWithIndex(keyIndex, 1);
            parent.decreaseNumKeys(1);
            right.shiftRecordsRight(current.getNumKeys());
            BTreeUtils.copyFromLeftNodeToRightNode(current, 0, right, 0, current.getNumKeys(), current.getNumKeys());
            right.increaseNumKeys(current.getNumKeys());
        }

        if (right.isLeaf()) {
            //fix list references
            right.setLeft(current.getLeft());
            if (right.getRight() != null) {
                right.getRight().setLeft(right);
            }
            current.setLeft(null);
            current.setRight(null);
        }
    }

    public static void mergeWithLeft(BTree tree, BTreeNode current, BTreeNode left) {
        BTreeNode parent = current.getParent();
        int keyIndex = parent.keyIndexOf(left, current);

        if (parent.isRoot() && parent.getNumKeys() == 1) {
            current.shiftRecordsRight(parent.getNumKeys());
            current.setKey(0, parent.getKey(0));
            //BTreeUtils.copyFromRightNodeToLeftNode(parent, 0, current, 0, parent.getNumKeys(), 0);
            current.increaseNumKeys(parent.getNumKeys());

            current.shiftRecordsRight(left.getNumKeys());
            BTreeUtils.copyNodeToAnother(left, current, 0);
            current.increaseNumKeys(left.getNumKeys());
            tree.setRoot(current);
            current.setParent(null);
        } else {
            parent.shiftRecordsLeftWithIndex(keyIndex, 1);
            parent.decreaseNumKeys(1);

            current.shiftRecordsRight(left.getNumKeys());
            BTreeUtils.copyNodeToAnother(left, current, 0);
            current.increaseNumKeys(left.getNumKeys());
        }

        if (current.isLeaf()) {
            current.setLeft(left.getLeft());
            if (current.getLeft() != null) {
                current.getLeft().setRight(current);
            }
            left.setLeft(null);
            left.setRight(null);
        }
    }

    public static void redistributeKeysFromRight(BTreeNode current, BTreeNode right) {
        int totalKeys = right.getNumKeys() + current.getNumKeys();
        int keysToMove = right.getNumKeys() - (totalKeys / 2);

        int startIndexRight = 0;
        int startIndexLeft = current.getNumKeys();
        //copy from left to current
        BTreeUtils.copyFromRightNodeToLeftNode(right, startIndexRight, current, startIndexLeft, keysToMove, keysToMove);

        //shift nodes in current node right
        right.shiftRecordsLeft(keysToMove);
        //fix number of keys
        right.decreaseNumKeys(keysToMove);
        current.increaseNumKeys(keysToMove);

        //move key from parent to current node
        BTreeNode parent = current.getParent();
        int parentKeyIndex = parent.keyIndexOf(current, right);
        if (current.isLeaf()) {
            parent.setKey(parentKeyIndex, right.getSmallestKey());
        } else {
            long aux = current.getLargestKey();
            current.setKey(keysToMove, parent.getKey(parentKeyIndex));
            parent.setKey(parentKeyIndex, aux);
        }
    }

    public static void redistributeKeysFromLeft(BTreeNode current, BTreeNode left) {
        int totalKeys = left.getNumKeys() + current.getNumKeys();
        int keysToMove = left.getNumKeys() - (totalKeys / 2) - 1;
        //shift nodes in current node right
//        keysToMove += 1;
        if (current.isLeaf()) {
            keysToMove += 1;
        }
        current.shiftRecordsRight(keysToMove);

        //int startIndexLeft = (totalKeys + 1) / 2;
        int startIndexLeft = left.getNumKeys() - keysToMove;
        int startIndexRight = 0;
        //copy from left to current
        BTreeUtils.copyFromLeftNodeToRightNode(left, startIndexLeft, current, startIndexRight, keysToMove, keysToMove);

        //fix number of keys
        left.decreaseNumKeys(keysToMove);
        current.increaseNumKeys(keysToMove);

        //move key from parent to current node
        BTreeNode parent = current.getParent();
        int parentKeyIndex = parent.keyIndexOf(left, current);
        if (current.isLeaf()) {
            parent.setKey(parentKeyIndex, current.getSmallestKey());
        } else {
            long aux = current.getSmallestKey();
            current.replaceKey(aux, parent.getKey(parentKeyIndex));
            //current.setKey(keysToMove, parent.getKey(parentKeyIndex));
            parent.setKey(parentKeyIndex, aux);
        }
    }


    public static void copyFromLeftNodeToRightNode(BTreeNode source,
                                                   int sourceStartIndex,
                                                   BTreeNode destination,
                                                   int destinationStartIndex,
                                                   int keys,
                                                   int children) {
        copyFromNodeToNode( source,
                            sourceStartIndex,
                            sourceStartIndex + 1,
                            destination,
                            destinationStartIndex,
                            destinationStartIndex,
                            keys,
                            children);
    }

    public static void copyFromRightNodeToLeftNode(BTreeNode source,
                                                   int sourceStartIndex,
                                                   BTreeNode destination,
                                                   int destinationStartIndex,
                                                   int keys,
                                                   int children) {
        copyFromNodeToNode( source,
                            sourceStartIndex,
                            sourceStartIndex,
                            destination,
                            destinationStartIndex,
                            destinationStartIndex,
                            keys,
                            children);
    }

    public static void copyFromNodeToNode(BTreeNode source, int srcStartK, int srcStartC, BTreeNode destination, int endStartK, int endStartC, int keys, int children) {
        System.arraycopy(source.getKeys(), srcStartK, destination.getKeys(), endStartK, keys);
        if (destination.isLeaf()) {
            System.arraycopy(source.getValues(), srcStartK, destination.getValues(), endStartK, keys);
        } else {
            System.arraycopy(source.getChildren(), srcStartC, destination.getChildren(), endStartC, children);
        }
    }

    public static void copyNodeToAnother(BTreeNode source, BTreeNode destination, int destinationIndex) {
        copyFromNodeToNode(source, 0, 0, destination, destinationIndex, destinationIndex, source.getNumKeys(), source.getNumKeys() + 1);
    }


}
