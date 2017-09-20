/*
 * Copyright 2009-2014 Tilmann Zaeschke. All rights reserved.
 *
 * This file is part of ZooDB.
 *
 * ZooDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ZooDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with ZooDB.  If not, see <http://www.gnu.org/licenses/>.
 *
 * See the README and COPYING files for further information.
 */
package org.zoodb.test.index2.btree;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeIterator;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeStorageBufferManager;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.prefix.BitOperationsHelper;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;

public class TestPrefixSharing {

    @Test
    public void testByteArrayToInt() {
    	int currentByte=0;
    	int i = Integer.MAX_VALUE;
    	byte[] outputArray = new byte[4];
    	
        outputArray[currentByte] = (byte) (i >> 24);
        outputArray[currentByte+1] = (byte) (i >> 16);
        outputArray[currentByte+2] = (byte) (i >> 8);
        outputArray[currentByte+3] = (byte) i;
        
    	assertEquals(i,PrefixSharingHelper.byteArrayToInt(outputArray, 0));
    }
    @Test
    public void testComputePrefix() {
        long[] arr = { 1500, 1800};

        long prefix = PrefixSharingHelper.computePrefix(arr);
        System.out.println(prefix);
    }

    @Test
    public void testSplitEqual() {
        long[] arr = {5, 5, 5, 5, 5, 5};
        long[] expectedLeft = {5, 5, 5};
        long[] expectedRight = {5, 5, 5};
        assertCorrectSplitAfterInsert(arr, expectedLeft, expectedRight);
    }

    @Test
    public void testSplitUnbalanced() {
        long[] arr = {5, 5, 5, 5, 5, 123};
        long[] expectedLeft = {5, 5, 5, 5, 5};
        long[] expectedRight = {123};
        assertCorrectSplitAfterInsert(arr, expectedLeft, expectedRight);
    }

    @Test
    public void testSplitLarge() {
        assertCorrectSplitAfterInsert(new long[]{1, 1, 2}, new long[]{1, 1}, new long[]{2});
    }

    @Test
    public void testSplitRedistributeLeftToRight() {
        long[] first = { 1, 2, 3, 4, 5 };
        long[] second = { 6};
        printSharedPrefixArrays(first, second);
        assertCorrectSplitForRedistributeLeftToRight(first, second);
    }

    @Test
    public void testSplitRedistributeRightToLeft() {
        long[] first = { 1, 2, 3};
        long[] second = { 4, 5, 6, 7, 8, 9};
        printSharedPrefixArrays(first, second);
        assertCorrectSplitForRedistributeRightToLeft(first, second);
    }

    @Test
    public void testSplitRedistributeRightToLeftEven() {
        long[] first = { 1};
        long[] second = { 2, 3, 4, 5, 6};
        printSharedPrefixArrays(first, second);
        assertCorrectSplitForRedistributeRightToLeft(first, second);
    }

    @Test
    public void testErrorRedistribute() {
        long[] first = { 44, 0, 0, 0, 0};
        long[] second = { 46, 48, 51, 53, 0};
        printSharedPrefixArrays(first, second);
        assertCorrectSplitForRedistributeRightToLeft(first, 1, second, 4);
    }

    @Test
    public void testIndividualBits() {
        long number = 11L;
        for (int i = 63; i >=0; i--) {
            System.out.print(BitOperationsHelper.getBitValue(number, i));
        }
    }

    @Test
    public void testEncodeDecode() {
        long[] inputArray = {-10, -5, 1};
        byte[] bytes = PrefixSharingHelper.encodeArray(inputArray);
        long[] decodedArray = PrefixSharingHelper.decodeArray(bytes);
        System.out.print(Arrays.toString(decodedArray));
        assertArrayEquals(inputArray, decodedArray);
        
        long[] inputArray2 = {5, 7, 8};
        byte[] encodedArray = PrefixSharingHelper.encodeArray(inputArray2);
        // [0, 0, 0, 3, 60, 0, 0, 0, 0, 0, 0, 0, -96, 30]
        decodedArray = PrefixSharingHelper.decodeArray(encodedArray);
        assertArrayEquals(inputArray2, decodedArray);
    }
    
    @Test
    public void testEmptyEncodeDecode() {
        long[] inputArray = {};
        byte[] bytes = PrefixSharingHelper.encodeArray(inputArray);
        System.out.print(Arrays.toString(bytes));
        long[] decodedArray = PrefixSharingHelper.decodeArray(bytes);
        assertArrayEquals(inputArray, decodedArray);
    }

    @Test
    public void testEncodeDecodeDuplicates() {
        long[] inputArray = {1, 1};
        byte[] bytes = PrefixSharingHelper.encodeArray(inputArray);
        long[] decodedArray = PrefixSharingHelper.decodeArray(bytes);
        System.out.print(Arrays.toString(decodedArray));
        assertArrayEquals(inputArray, decodedArray);
    }

    @Test
    public void testComputedSizesOfChildrenInsert() {
        int pageSize = 256;
        UniquePagedBTree tree = (UniquePagedBTree) createEmptyBTree(pageSize, true);
        List<LongLongIndex.LLEntry> entries = BTreeTestUtils.randomUniqueEntries(50000,
                42);
        Collections.shuffle(entries, new Random(System.nanoTime()));
        for (LongLongIndex.LLEntry entry : entries) {
            //insert a new value
            tree.insert(entry.getKey(), entry.getValue());

            //check the all nodes have proper value
            BTreeIterator iterator = new BTreeIterator(tree);
            while (iterator.hasNext()) {
                BTreeNode node = iterator.next();
                if (!node.isLeaf()) {
                    for (int i = 0; i <= node.getNumKeys(); i++) {
                        assertEquals("Computed child size not correct",
                                 node.getChild(i).getCurrentSize(), node.getChildSize(i));
                    }
                }
            }
        }
    }

    @Test
    public void testComputedSizesOfChildrenInsertDelete() {
        int pageSize = 256;
        UniquePagedBTree tree = (UniquePagedBTree) createEmptyBTree(pageSize, true);
        List<LongLongIndex.LLEntry> entries = BTreeTestUtils.randomUniqueEntries(50000,
                42);
        Collections.shuffle(entries, new Random(System.nanoTime()));
        for (LongLongIndex.LLEntry entry : entries) {
            //insert a new value
            tree.insert(entry.getKey(), entry.getValue());

            //check the all nodes have proper value
            BTreeIterator iterator = new BTreeIterator(tree);
            while (iterator.hasNext()) {
                BTreeNode node = iterator.next();
                if (!node.isLeaf()) {
                    for (int i = 0; i <= node.getNumKeys(); i++) {
                        assertEquals("Computed child size not correct",
                                node.getChild(i).getCurrentSize(), node.getChildSize(i));
                    }
                }
            }
        }

        for (LongLongIndex.LLEntry entry : entries) {
            //insert a new value
            tree.delete(entry.getKey());
            //check the all nodes have proper value
            BTreeIterator iterator = new BTreeIterator(tree);
            while (iterator.hasNext()) {
                BTreeNode node = iterator.next();
                if (!node.isLeaf()) {
                    for (int i = 0; i <= node.getNumKeys(); i++) {
                        assertEquals("Computed child size not correct",
                                node.getChildSize(i), node.getChild(i).getCurrentSize());
                    }
                }
            }
        }
    }

    @Test
    public void testProperSizeAfterInsert() {
        int pageSize = 128;
        UniquePagedBTree tree = (UniquePagedBTree) createEmptyBTree(pageSize, true);
        List<LongLongIndex.LLEntry> entries = BTreeTestUtils.randomUniqueEntries(1000,
                Calendar.getInstance().getTimeInMillis());
        for (LongLongIndex.LLEntry entry : entries) {
            //insert a new value
            tree.insert(entry.getKey(), entry.getValue());

            //check the all nodes have proper value
            BTreeIterator iterator = new BTreeIterator(tree);
            while (iterator.hasNext()) {
                BTreeNode node = iterator.next();
                int currentNodeSize = node.computeSize();
                assertTrue("Current node size is too large. " +
                        "\nCurrent node size: " + currentNodeSize +
                        "\nMaximum size: " + pageSize,
                        currentNodeSize <= pageSize);
            }
        }
    }

    @Test
    public void testProperSizeAfterInsertDelete() {
        int pageSize = 128;
        UniquePagedBTree tree = (UniquePagedBTree) createEmptyBTree(pageSize, true);
        List<LongLongIndex.LLEntry> entries = BTreeTestUtils.randomUniqueEntries(50000,
                42);
        for (LongLongIndex.LLEntry entry : entries) {
            //insert a new value
            tree.insert(entry.getKey(), entry.getValue());

            //check the all nodes have proper value
            BTreeIterator iterator = new BTreeIterator(tree);
            while (iterator.hasNext()) {
                BTreeNode node = iterator.next();
                int currentNodeSize = node.computeSize();
                assertTrue("Current node size is too large. " +
                        "\nCurrent node size: " + currentNodeSize +
                        "\nMaximum size: " + pageSize,
                        currentNodeSize <= pageSize);
            }
        }

        for (LongLongIndex.LLEntry entry : entries) {
            //insert a new value
            tree.delete(entry.getKey());
            //check the all nodes have proper value
            BTreeIterator iterator = new BTreeIterator(tree);
            while (iterator.hasNext()) {
                BTreeNode node = iterator.next();
                int currentNodeSize = node.computeSize();
                assertTrue("Current node size is too large. " +
                        "\nCurrent node size: " + currentNodeSize +
                        "\nMaximum size: " + pageSize,
                        currentNodeSize <= pageSize);
            }
        }
    }

    private BTree createEmptyBTree(int pageSize, boolean unique) {
        IOResourceProvider storage = new StorageRootInMemory(pageSize).createChannel();
        BTreeBufferManager bufferManager = new BTreeStorageBufferManager(storage, unique);
        if (unique) {
            return new UniquePagedBTree(pageSize, bufferManager);
        } else {
            return new NonUniquePagedBTree(pageSize, bufferManager);
        }
    }

    private void assertCorrectSplitForRedistributeRightToLeft(long[] first, long[] second) {
        assertCorrectSplitForRedistributeRightToLeft(first, first.length, second, second.length);
    }

    private void assertCorrectSplitForRedistributeRightToLeft(long[] first, int firstLength, long[] second, int secondLength){
        int splitPosition = PrefixSharingHelper.computeIndexForRedistributeRightToLeft(first, firstLength, second, secondLength);
        long[] secondAfterSplit = Arrays.copyOfRange(second, splitPosition + 1, secondLength);
        long[] firstAfterSplit = new long[firstLength + splitPosition + 1];
        System.arraycopy(first, 0, firstAfterSplit, 0, firstLength);
        System.arraycopy(second, 0, firstAfterSplit, firstLength, splitPosition + 1);
        printSharedPrefixArrays(firstAfterSplit, secondAfterSplit);
    }

    public void assertCorrectSplitForRedistributeLeftToRight(long[] first, long[] second) {
        assertCorrectSplitForRedistributeLeftToRight(first, first.length,  second, second.length);
    }
    public void assertCorrectSplitForRedistributeLeftToRight(long[] first, int firstLength, long[] second, int secondLength) {
        int splitPosition = PrefixSharingHelper.computeIndexForRedistributeLeftToRight(first, firstLength, second, secondLength);
        long[] firstAfterSplit = Arrays.copyOfRange(first, 0, splitPosition + 1);
        long[] secondAfterSplit = new long[firstLength - firstAfterSplit.length + secondLength];
        System.arraycopy(first, splitPosition + 1, secondAfterSplit, 0, (firstLength - firstAfterSplit.length));
        System.arraycopy(second, 0, secondAfterSplit, (firstLength - firstAfterSplit.length), secondLength);
        printSharedPrefixArrays(firstAfterSplit, secondAfterSplit);
    }

    public void assertCorrectSplitAfterInsert(long[] arr, long[] expectedLeft, long[] expectedRight) {
        int splitIndex = PrefixSharingHelper.computeIndexForSplitAfterInsert(arr);
        long[] arrLeft = Arrays.copyOfRange(arr, 0, splitIndex + 1);
        long[] arrRight = Arrays.copyOfRange(arr, splitIndex + 1, arr.length);
        assertArrayEquals(expectedLeft, arrLeft);
        assertArrayEquals(expectedRight, arrRight);
        System.out.println("Detailed split information for array: " + Arrays.toString(arr));
        System.out.println("Index:\t" + splitIndex);
        printSharedPrefixArrays(arrLeft, arrRight);
    }

    private static void printSharedPrefixArrays(long[] arrLeft, long[] arrRight) {
        System.out.println("Left:\t" + Arrays.toString(arrLeft));
        System.out.println("Right:\t" + Arrays.toString(arrRight));
        PrefixSharingHelper.printSharedPrefixArray(arrLeft);
        PrefixSharingHelper.printSharedPrefixArray(arrRight);
    }

}
