package org.zoodb.internal.server.index.btree.prefix;

public class PrefixSharingHelper {

    /**
     * Compute the size of the bit prefix shared by the two long values.
     *
     * @param first
     * @param last
     * @return
     */
    public static long computePrefix(long first, long last) {
        if (first == last) {
            return 64;
        }
        long prefix = 0;
        long low = 0;
        long high = 63;
        long mid = 0;
        while (low <= high) {
            mid = low + ((high - low) >> 1);
            long firstPrefix = first >> (64 - mid);
            long lastPrefix = last >> (64 - mid);
            if (firstPrefix == lastPrefix) {
                low = mid + 1;
                prefix = mid;
            } else {
                high = mid - 1;
            }
        }
        return prefix;
    }

    /**
     * Computes the bit prefix of the array arr. The array has to be sorted prior
     * to this operation.
     * @param arr        The array received as argument.
     * @return           The bit prefix
     */
    public static long computePrefix(long[] arr) {

        long first = arr[0];
        long last = arr[arr.length - 1];
        long prefix = computePrefix(first, last);
        System.out.println(String.format("First:\t %d\t %-72s",first, toBinaryLongString(first)));
        System.out.println(String.format("Last:\t %d\t %-72s",last, toBinaryLongString(last)));
        System.out.println(String.format("Prefix:\t %d\t %-72s",prefix, toBinaryLongString(first >> (64 - prefix))));
        return prefix;
    }

    /**
     * Computes the optimal split point for a prefix shared array after inserting
     * a new element newElement. The optimal split point is the index that splits
     * the array into two prefix shared arrays of relatively equal size.
     *
     * @param arr               The prefix shared array.
     * @return
     */
    public static int computePrefixForSplitAfterInsert(long[] arr) {
        /*
         *  Perform a binary search by computing the sizes of the left and right array
         *  after splitting by a certain index.
         *
         *  If the left array has a larger size, move the splitting point to the right.
         */
        int low = 0 ;
        int high = arr.length - 1;
        int mid = 0;
        int optimalIndex = 0;
        long optimalDiff = Long.MAX_VALUE;
        while (low <= high) {
            mid = low + ((high - low) >> 1);
            long prefixLeft = computePrefix(arr[0], arr[mid]);
            long prefixRight = computePrefix(arr[mid+1], arr[arr.length - 1]);
            long sizeLeft = prefixLeft + (mid + 1) * (64 - prefixLeft);
            long sizeRight = prefixRight + (arr.length - 1 - mid) * (64 - prefixRight);
            if (optimalDiff > Math.abs(sizeLeft - sizeRight)) {
                optimalIndex = mid;
                optimalDiff = Math.abs(sizeLeft - sizeRight);
            }
            if (sizeLeft < sizeRight) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        System.out.println("Optimal difference: " + optimalDiff);
        return optimalIndex;
    }

    public static void printSharedPrefixArray(long[] arr) {
        long prefix = computePrefix(arr);
        System.out.println("Prefix size:\t" + prefix);
        System.out.println("Array size:\t" + (64 - prefix) * arr.length);
        System.out.println("Total size:\t" + (prefix + ((64 - prefix) * arr.length)));
    }

    /**
     * Print a 64 character representation of a long value.
     * @param number
     * @return
     */
    public static String toBinaryLongString(long number) {
        String binaryString = Long.toBinaryString(number);
        int padding = 64 - binaryString.length();
        StringBuffer paddedBinaryString = new StringBuffer();
        for (int i = 0; i < padding; i++) {
            paddedBinaryString.append("0");
        }
        return paddedBinaryString.append(binaryString).toString();
    }
}
