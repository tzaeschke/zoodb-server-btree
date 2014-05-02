package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.prefix.BitOperationsHelper;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
        long[] arr = {5, 5, 5, 5, 5, 6};
        long[] expectedLeft = {5, 5, 5, 5, 5};
        long[] expectedRight = {6};
        assertCorrectSplitAfterInsert(arr, expectedLeft, expectedRight);
    }

    @Test
    public void testSplitLarge() {
        assertCorrectSplitAfterInsert(new long[]{1, 1, 2}, new long[]{1, 1}, new long[]{2});
    }

    @Test
    public void testSplitRedistributeLeftToRight() {
        long[] first = { 1, 2, 3, 4, 5 };
        long[] second = { 8, 9};
        printSharedPrefixArrays(first, second);
        assertCorresSplitForRedistributeLeftToRight(first, second);
    }

    @Test
    public void testSplitRedistributeRightToLeft() {
        long[] first = { 1, 2, 3};
        long[] second = { 4, 5, 6, 7, 8, 9};
        printSharedPrefixArrays(first, second);
        assertCorresSplitForRedistributeRightToLeft(first, second);
    }

    @Test
    public void testSplitRedistributeRightToLeftEven() {
        long[] first = { 1 };
        long[] second = { 1, 1, 1, 1, 1};
        printSharedPrefixArrays(first, second);
        assertCorresSplitForRedistributeRightToLeft(first, second);
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
        decodedArray = PrefixSharingHelper.decodeArray(PrefixSharingHelper.encodeArray(inputArray2));
        System.out.print(Arrays.toString(decodedArray));
        assertArrayEquals(inputArray2, decodedArray);
    }

    @Test
    public void testEncodeDecodeDuplicates() {
        long[] inputArray = {1, 1};
        byte[] bytes = PrefixSharingHelper.encodeArray(inputArray);
        long[] decodedArray = PrefixSharingHelper.decodeArray(bytes);
        System.out.print(Arrays.toString(decodedArray));
        assertArrayEquals(inputArray, decodedArray);
    }


    private void assertCorresSplitForRedistributeRightToLeft(long[] first, long[] second) {
        int splitPosition = PrefixSharingHelper.computeIndexForRedistributeRightToLeft(first, second);
        long[] secondAfterSplit = Arrays.copyOfRange(second, splitPosition, second.length);
        long[] firstAfterSplit = new long[first.length + second.length - secondAfterSplit.length];
        System.arraycopy(first, 0, firstAfterSplit, 0, first.length);
        System.arraycopy(second, 0, firstAfterSplit, first.length, splitPosition);
        printSharedPrefixArrays(firstAfterSplit, secondAfterSplit);
    }

    public void assertCorresSplitForRedistributeLeftToRight(long[] first, long[] second) {
        int splitPosition = PrefixSharingHelper.computeIndexForRedistributeLeftToRight(first, second);
        long[] firstAfterSplit = Arrays.copyOfRange(first, 0, splitPosition + 1);
        long[] secondAfterSplit = new long[first.length - firstAfterSplit.length + second.length];
        System.arraycopy(first, splitPosition + 1, secondAfterSplit, 0, (first.length - firstAfterSplit.length));
        System.arraycopy(second, 0, secondAfterSplit, (first.length - firstAfterSplit.length), second.length);
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

//    @Test
//    public void testInsertInOrderedArray() {
//        long[] keys = new long[] { 1, 3, 5, 7, 9, 0, 0, 0, 0};
//        long[] expectedKeys = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9};
//        keys = insertedOrderedInArray(2, keys, 5);
//        keys = insertedOrderedInArray(4, keys, 6);
//        keys = insertedOrderedInArray(6, keys, 7);
//        keys = insertedOrderedInArray(8, keys, 8);
//        assertArrayEquals(expectedKeys, keys);
//    }

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
