package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class TestPrefixSharing {

    @Test
    public void testComputePrefix() {
        long[] arr = { 1114, 11116};

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
    public void testSplitRedistribute() {
        long[] first = { 1, 2, 3, 4, 5 };
        long[] second = { 8, 9};
        printSharedPrefixArrays(first, second);
        assertCorresSplitForRedistribute(first, second);
    }

    public void assertCorresSplitForRedistribute(long[] first, long[] second) {
        int splitPosition = PrefixSharingHelper.computeIndexForSplit(first, second);
        long[] firstAfterSplit = Arrays.copyOfRange(first, 0, splitPosition + 1);
        long[] secondAfterSplit = new long[first.length - firstAfterSplit.length + second.length];
        System.arraycopy(first, splitPosition + 1, secondAfterSplit, 0, (first.length - firstAfterSplit.length));
        System.arraycopy(second, 0, secondAfterSplit, (first.length - firstAfterSplit.length), second.length);
        printSharedPrefixArrays(firstAfterSplit, secondAfterSplit);
    }

    public void assertCorrectSplitAfterInsert(long[] arr, long[] expectedLeft, long[] expectedRight) {
        int splitIndex = PrefixSharingHelper.computePrefixForSplitAfterInsert(arr);
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
