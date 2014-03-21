package org.zoodb.internal.server.index.btree;

import org.zoodb.internal.util.Pair;

/**
 * Represents the node of a B+ tree.
 *
 * Support for linked-lists of nodes on the leaf level is yet to be added.
 */
public abstract class BTreeNode {

    private final boolean isLeaf;
    private final int order;

    //ToDo maybe we want to have the keys set dynamically sized somehow
    private int numKeys;
    private long[] keys;

    private long[] values;

    public BTreeNode(BTreeNode parent, int order, boolean isLeaf) {
        setParent(parent);
        this.order = order;
        this.isLeaf = isLeaf;

        initKeys(order);
        if (isLeaf) {
            initValues(order);
        } else {
            initChildren(order);
        }
    }

    /**
     * Returns the index + 1 of the key received as an argument. If the key is not in the array, it will return
     * the index of the smallest key in the array that is larger than the key received as argument.
     *
     * @param key
     * @return
     */
    public int findKeyPos(long key) {
        //Todo make method package and add test to the same package
        if (getNumKeys() == 0) {
            return 0;
        }
        int low = 0;
        int high = getNumKeys() - 1;
        int mid = 0;
        boolean found = false;
        //perform binary search
        while (!found && low <= high) {
            mid = low + (high - low) / 2;
            if (getKey(mid) == key) {
                found = true;
            } else {
                if (key < getKey(mid)) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
        }

        //if the key is not here, find the child subtree that has it
        if (!found) {
            if (mid == 0 && key < getKey(0)) {
                return 0;
            } else if (key < getKey(mid)) {
                return mid;
            }
        }
        return mid + 1;
    }

    /**
     * Find the value corresponding to a key in a leaf node.
     *
     * @param key   The key received as argument
     * @return      The value corresponding to the key in the index.
     *              If the key is not found in the index, -1 is returned.
     */
    public long findValue(long key) {
        if (!this.isLeaf()) {
            throw new IllegalStateException("Should only be called on leaf nodes.");
        }
        if (getNumKeys() == 0) {
            return 0;
        }
        int low = 0;
        int high = getNumKeys() - 1;
        int mid = 0;
        boolean found = false;
        while (!found && low <= high) {
            mid = low + (high - low) / 2;
            if (getKey(mid) == key) {
                found = true;
            } else {
                if (key < getKey(mid)) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
        }
        if (!found) {
            return -1;
        }
        return getValue(mid);
    }

    public boolean containsKey(long key) {
        if (getNumKeys() == 0) {
            return false;
        }
        int low = 0;
        int high = getNumKeys() - 1;
        int mid = 0;
        boolean found = false;
        while (!found && low <= high) {
            mid = low + (high - low) / 2;
            if (getKey(mid) == key) {
                found = true;
            } else {
                if (key < getKey(mid)) {
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
        }
        return found;
    }

    public BTreeNode findChild(long key) {
        return getChild(findKeyPos(key));
    }

    /**
     * Leaf put.
     *
     * Requires that node is not full.
     * @param key
     * @param value
     */
    public void put(long key, long value) {
        if (!isLeaf()) {
            throw new IllegalStateException("Should only be called on leaf nodes.");
        }

        int pos = findKeyPos(key);
        if(pos > numKeys && keys[pos] == key) {
            throw new IllegalStateException("Tree is not allowed to have non-unique keys.");
        }
        shiftRecords(pos, pos+1, getNumKeys() - pos);
        setKey(pos, key);
        setValue(pos, value);
        incrementNumKyes();
    }

    /**
     * Inner-node put. Places key to the left of the next bigger key k'.
     *
     * Requires that 
     * 		key <= keys(newNode)
     * 		all elements of the left child of k' are  smaller than key 
     * 		node is not full.
     * Assumes that leftOf(key') <= keys(newNode) 
     * @param key
     * @param newNode
     */
    public void put(long key, BTreeNode newNode) {
        if (isLeaf()) {
            throw new IllegalStateException("Should only be called on inner nodes.");
        } else if(getNumKeys() == 0) {
            throw new IllegalStateException("Should only be called when node is non-empty.");
        }

        int pos = findKeyPos(key);
        if(pos > numKeys && keys[pos] == key) {
            throw new IllegalStateException("Tree is not allowed to have non-unique keys.");
        }
        int recordsToMove = getNumKeys() - pos;
        shiftChildren(pos + 1, pos + 2, recordsToMove);
        setChild(pos + 1, newNode);
        newNode.setParent(this);

        shiftKeys(pos, pos + 1, recordsToMove);
        setKey(pos, key);
        incrementNumKyes();
    }


    /**
     * Root-node put.
     *
     * Used when a non-leaf root is empty and will be populated by a single key and two nodes.
     * @param key       The new key on the root.
     * @param left      The left node.
     * @param right     The right node.
     */
    public void put(long key, BTreeNode left, BTreeNode right) {
        if (!isRoot()) {
            throw new IllegalStateException("Should only be called on the root node.");
        }
        setKey(0, key);
        setNumKeys(1);

        setChild(0, left);
        setChild(1, right);;
        
        left.setParent(this);
        right.setParent(this);
    }

    /**
     * Puts a new key into the node and splits accordingly. Returns the newly created leaf, which is to the right.
     * 
     * @param newKey
     * @return
     */
    public BTreeNode putAndSplit(long newKey, long value) {
        if (!isLeaf()) {
            throw new IllegalStateException("Should only be called on leaf nodes.");
        }
        BTreeNode tempNode = newNode(null, order + 1, true);
        System.arraycopy(getKeys(), 0, tempNode.getKeys(), 0, getNumKeys());
        System.arraycopy(getValues(), 0, tempNode.getValues(), 0, getNumKeys());
        tempNode.setNumKeys(getNumKeys());
        tempNode.put(newKey, value);

        int keysInLeftNode = (int) Math.ceil((order) / 2.0);
        int keysInRightNode = order  - keysInLeftNode;
        
        //populate left node
        System.arraycopy(tempNode.getKeys(), 0, getKeys(), 0, keysInLeftNode);
        System.arraycopy(tempNode.getValues(), 0, getValues(), 0, keysInLeftNode);
        setNumKeys(keysInLeftNode);

        //populate right node
        BTreeNode rightNode = newNode(getParent(), order, true);
        rightNode.setParent(getParent());
        System.arraycopy(tempNode.getKeys(), keysInLeftNode, rightNode.getKeys(), 0, keysInRightNode);
        System.arraycopy(tempNode.getValues(), keysInLeftNode, rightNode.getValues(), 0, keysInRightNode);
        rightNode.setNumKeys(keysInRightNode);

        //fix references
        rightNode.setLeft(this);
        rightNode.setRight(this.getRight());
        this.setRight(rightNode);
        if (rightNode.getRight() != null) {
            rightNode.getRight().setLeft(rightNode);
        }
        return rightNode;
    }

    /**
     * Puts a key and a new node to the inner structure of the tree.
     *
     * @param key
     * @param newNode
     * @return
     */
    public Pair<BTreeNode, Long> putAndSplit(long key, BTreeNode newNode) {
        if (isLeaf()) {
            throw new IllegalStateException("Should only be called on inner nodes.");
        }

        //create a temporary node to allow the insertion
        BTreeNode tempNode = newNode(null, order + 1, false);
        System.arraycopy(getKeys(), 0, tempNode.getKeys(), 0, getNumKeys());
        System.arraycopy(getChildren(), 0, tempNode.getChildren(), 0, order);
        tempNode.setNumKeys(getNumKeys());
        tempNode.put(key, newNode);

        //split
        BTreeNode right = newNode(getParent(), order, false);
        int keysInLeftNode = (int) Math.floor(order / 2.0);
        //populate left node
        System.arraycopy(tempNode.getKeys(), 0, getKeys(), 0, keysInLeftNode);
        System.arraycopy(tempNode.getChildren(), 0, getChildren(), 0, keysInLeftNode + 1);
        setNumKeys(keysInLeftNode);

        //populate right node
        int keysInRightNode = order-keysInLeftNode-1;
        System.arraycopy(tempNode.getKeys(), keysInLeftNode + 1, right.getKeys(), 0, keysInRightNode);
        System.arraycopy(tempNode.getChildren(), keysInLeftNode + 1, right.getChildren(), 0, keysInRightNode+1);
        right.setNumKeys(keysInRightNode);

        long keyToMoveUp = tempNode.getKeys()[keysInLeftNode];

        //update children pointers
        newNode.setParent(key < keyToMoveUp ? this : right);
        for (int i = keysInLeftNode + 1; i < order + 1; i++) {
            tempNode.getChildren()[i].setParent(right);
        }
        right.setParent(getParent());

        return new Pair<>(right, keyToMoveUp);
    }

    /**
     * Delete the key from the node.
     * @param key
     */
    public void delete(long key) {
        if (!isLeaf()) {
            throw new IllegalStateException("Should be a leaf node");
        }
        final int keyPos = findKeyPos(key);
        int recordsToMove = getNumKeys() - keyPos;
        shiftRecords(keyPos, keyPos - 1, recordsToMove);
        decrementNumKeys();

    }

    public void setKey(int index, long key) {
        getKeys()[index] = key;
    }

    public void setValue(int index, long value) {
        getValues()[index] = value;
    }

    private void shiftRecords(int startIndex, int endIndex, int amount) {
        shiftKeys(startIndex, endIndex, amount);
        if (isLeaf()) {
            shiftValues(startIndex, endIndex, amount);
        } else {
            shiftChildren(startIndex, endIndex, amount + 1);
        }
    }

    public void shiftRecordsRight(int amount) {
        shiftKeys(0, amount, getNumKeys());
        if (isLeaf()) {
            shiftValues(0, amount, getNumKeys());
        } else {
            shiftChildren(0, amount, getNumKeys() + 1);
        }
    }

    public void shiftRecordsLeftWithIndex(int startIndex, int amount) {
        int keysToMove = getNumKeys() - amount;
        shiftKeys(startIndex + amount, startIndex, keysToMove);
        if (isLeaf()) {
            shiftValues(startIndex + amount, startIndex, keysToMove);
        } else {
            shiftChildren(startIndex + amount, startIndex, getNumKeys() + 1);
        }
    }

    public void shiftRecordsLeft(int amount) {
        shiftRecordsLeftWithIndex(0, amount);
    }

    private void shiftKeys(int startIndex, int endIndex, int amount) {
        System.arraycopy(getKeys(), startIndex, getKeys(), endIndex, amount);
    }

    private void shiftValues(int startIndex, int endIndex, int amount) {
        System.arraycopy(getValues(), startIndex, getValues(), endIndex, amount);
    }

    private void shiftChildren(int startIndex, int endIndex, int amount) {
        System.arraycopy(getChildren(), startIndex, getChildren(), endIndex, amount);
    }

    public int keyIndexOf(BTreeNode left, BTreeNode right) {
        for (int i = 0; i < getNumKeys(); i++) {
            if (getChild(i) == left && getChild(i + 1) == right) {
                return  i;
            }
        }
        return -1;
    }

    public boolean isUnderfull() {
        if (isRoot()) {
            return getNumKeys() == 0;
        }
        if (isLeaf()) {
            return getNumKeys() < minKeysAmount();
        }
        return getNumKeys() + 1 < order / 2.0;
    }

    public boolean hasExtraKeys() {
        if (isRoot()) {
            return true;
        }
        return getNumKeys() - 1 >= minKeysAmount();
    }

    public boolean isOverflowing() {
        return getNumKeys() >= order;
    }

    public BTreeNode leftSibling() {
        return getParent().leafSiblingOf(this);
    }

    public BTreeNode rightSibling() {
        return getParent().rightSiblingOf(this);
    }

    private BTreeNode leafSiblingOf(BTreeNode node) {
        int index = 0;
        for (BTreeNode child : getChildren()) {
            if (child == node) {
                if (index == 0) {
                    return null;
                } else {
                    return getChildren()[index - 1];
                }
            }
            index++;
        }
        return null;
    }

    private BTreeNode rightSiblingOf(BTreeNode node) {
        int index = 0;
        for (BTreeNode child : getChildren()) {
            if (child == node) {
                if (index == getNumKeys()) {
                    return null;
                } else {
                    return getChildren()[index + 1];
                }
            }
            index++;
        }
        return null;
    }

    public void replaceKey(long key, long replacementKey) {
        if (replacementKey < key) {
            throw new RuntimeException("Replacing " + key + " with " + replacementKey + " might be illegal.");
        }
        int pos = findKeyPos(key);
        if (pos > -1) {
            setKey(pos - 1, replacementKey);
        }
    }

    public boolean incrementNumKyes() {
        return increaseNumKeys(1);
    }

    public boolean decrementNumKeys() {
        return decreaseNumKeys(1);
    }

    public boolean increaseNumKeys(int amount) {
        int newNumKeys = getNumKeys() + amount;
        if (newNumKeys > getKeys().length) {
            return false;
        }
        setNumKeys(newNumKeys);
        return true;
    }

    public boolean decreaseNumKeys(int amount) {
        int newNumKeys = getNumKeys() - amount;
        if (newNumKeys < 0) {
            return false;
        }
        setNumKeys(newNumKeys);
        return true;
    }

    private void initKeys(int order) {
        setKeys(new long[order - 1]);
        setNumKeys(0);
    }

    private void initValues(int order) {
        setValues( new long[order - 1]);
    }

    private void initChildren(int order) {
        setChildren(new BTreeNode[order]);
    }


    public long getValue(int index) {
        return getValues()[index];
    }

    public long getKey(int index) {
        return getKeys()[index];
    }

    public BTreeNode getChild(int index) {
        return getChildren()[index];
    }

    public void setChild(int index, BTreeNode child) {
        getChildren()[index] = child;
    }

    public double minKeysAmount() {
        return (order - 1) / 2.0D;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public boolean isRoot() {
        return getParent() == null;
    }


    public long getSmallestKey() {
    	return keys[0];
    }

    public long getLargestKey() {
        return keys[numKeys - 1];
    }

    public long smallestKey() {
        return keys[0];
    }

    public long largestKey() {
        return keys[numKeys - 1];
    }

    public String toString() {
    	String ret = (isLeaf() ? "leaf" : "inner") + "-node: k:";
    	ret += "[";
    	for (int i=0; i < this.getNumKeys(); i++) {
    		ret += Long.toString(keys[i]);
    		if(i!=this.getNumKeys()-1)
    			ret+= " ";
    	}
    	ret+= "]";
    	if(isLeaf()) {
    		ret += ",   \tv:"; 
	    	ret += "[";
	    	for (int i=0; i < this.getNumKeys(); i++) {
	    		ret += Long.toString(values[i]);
	    		if(i!=this.getNumKeys()-1)
	    			ret+= " ";
	    	}
	    	ret += "]";
    	} else {
    		ret+= "\n\tc:"; 
	    	for (int i=0; i < this.getNumKeys()+1; i++) {
				String[] lines = this.getChild(i).toString().split("\r\n|\r|\n");
			for(String l : lines) {
					ret+="\n\t" + l;
				}
    		}
    	}
    	return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BTreeNode)) return false;

        BTreeNode bTreeNode = (BTreeNode) o;
        
        if (isLeaf() != bTreeNode.isLeaf()) return false;
        if (getNumKeys() != bTreeNode.getNumKeys()) return false;
        if (order != bTreeNode.order) return false;
        if (!arrayEquals(getChildren(), bTreeNode.getChildren(), getNumKeys() + 1)) return false;
        if (!arrayEquals(getKeys(), bTreeNode.getKeys(), getNumKeys())) return false;
        // checking for parent equality would result in infinite loop
        // if (parent != null ? !parent.equals(bTreeNode.parent) : bTreeNode.parent != null) return false;
        if (!arrayEquals(getValues(), bTreeNode.getValues(), getNumKeys())) return false;

        return true;
    }

    private <T> boolean arrayEquals(T[] first, T[] second, int size) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }
        if (first.length < size || second.length < size) {
            return false;
        }

        for (int i = 0; i < size; i++) {
            if ( (first[i] != second[i] ) && (first[i] != null ) && (!first[i].equals(second[i]))) {
                return false;
            }
        }
        return true;
    }

    private boolean arrayEquals(long[] first, long[] second, int size) {
        if (first == second) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }
        if (first.length < size || second.length < size) {
            return false;
        }

        for (int i = 0; i < size; i++) {
            if (!(first[i] == second[i])) {
                return false;
            }
        }
        return true;
    }

    public int getNumKeys() {
        return numKeys;
    }

    public abstract BTreeNode getParent();

    public long[] getKeys() {
        return keys;
    }

    public long[] getValues() {
        return values;
    }

    public void setNumKeys(int numKeys) {
        this.numKeys = numKeys;
    }

    public abstract void setParent(BTreeNode parent);

    public abstract BTreeNode[] getChildren();

    public abstract void setChildren(BTreeNode[] children);

    public void setKeys(long[] keys) {
    	this.keys = keys;
    }

    public void setValues(long[] values) {
        this.values = values;
    }

    public int getOrder() {
        return order;
    }

    public abstract void setLeft(BTreeNode left);

    public abstract void setRight(BTreeNode right);

    public abstract BTreeNode getLeft();

    public abstract BTreeNode getRight();
    
    public abstract BTreeNode newNode(BTreeNode parent, int order, boolean isLeaf);
}
