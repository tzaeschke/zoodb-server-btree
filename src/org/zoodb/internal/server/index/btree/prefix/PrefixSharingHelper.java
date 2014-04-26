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

    /**
     * Compute the number of keys to move from the left array to the right array.
     *
     * As a precondition, the left array should have a larger storage size than the
     * left array.
     *
     * @param first
     * @param second
     * @return
     */
    public static int computeIndexForSplit(long[] first, long[] second) {
        /*
         *  Perform a binary search on the index in the first array that would
         *  provide the optimal split point.
         */
        int low = 0 ;
        int high = first.length - 1;
        int mid = 0;
        int optimalIndex = 0;
        long optimalDiff = Long.MAX_VALUE;
        while (low <= high) {
            mid = low + ((high - low) >> 1);
            long prefixLeft = computePrefix(first[0], first[mid]);
            long prefixRight = computePrefix(first[mid+1], second[second.length - 1]);
            long sizeLeft = prefixLeft + (mid + 1) * (64 - prefixLeft);
            long sizeRight = prefixRight + (first.length - 1 - mid + second.length) * (64 - prefixRight);
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

    /**
     * Utility method for printing the prefix representation of an array.
     *
     * @param arr
     */
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

    public static byte[] encodeArray(long[] array) {
        long prefix = computePrefix(array);
        return encodeArray(array, prefix);
    }

    public static long prefixBits(long prefix, long number) {
        return number >> (64 - prefix);
    }

    /**
     * Encode a prefix shared long array into an array of bytes.
     *
     * @param array
     * @param prefix
     * @return
     */
    public static byte[] encodeArray(long[] array, long prefix) {
        int inputArrayIndex = 0;
        int currentByte = 0;
        int indexInCurrentByte = 0;

        /* Compute the number of bits to be stored */
        int bitsToStore = (int) (prefix + (64 - prefix) * array.length);
        int outputArraySize = (int) Math.ceil(bitsToStore/ 8.0);

        byte[] outputArray = new byte[outputArraySize + 5];

        /*Write the size of the array as an int - always 4 bytes */
        outputArray[currentByte++] = (byte) (array.length >>> 24);
        outputArray[currentByte++] = (byte) (array.length >>> 16);
        outputArray[currentByte++] = (byte) (array.length >>> 8);
        outputArray[currentByte++] = (byte) array.length;

        /* Write the prefix size */
        outputArray[currentByte++] = (byte) prefix;

        long prefixBits = prefixBits(prefix, array[0]);
        /* Encode the prefix*/
        for (int i = (int) (prefix - 1); i >= 0; i--) {
            long bitValue = BitOperationsHelper.getBitValue(prefixBits, i);
            outputArray[currentByte] = BitOperationsHelper.setBitValue(outputArray[currentByte], indexInCurrentByte, bitValue);
            indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
            currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
        }

        /* Perform the actual encoding */
        while (inputArrayIndex < array.length) {
            for (int i = (int) (63 - prefix); i >= 0; i--) {
                long bitValue = BitOperationsHelper.getBitValue(array[inputArrayIndex], i);
                outputArray[currentByte] = BitOperationsHelper.setBitValue(outputArray[currentByte], indexInCurrentByte, bitValue);
                indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
                currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
            }
            inputArrayIndex++;
        }
        return outputArray;
    }

    /**
     * Decoded a prefix shared encoded array
     * @param encodedArray
     * @return
     */
    public static long[] decodeArray(byte[] encodedArray) {
        int currentByte = 0;
        int decodedArraySize =  (encodedArray[currentByte] << 24 )  |
                                (encodedArray[currentByte+1] << 16 )  |
                                (encodedArray[currentByte+2] << 8 )   |
                                encodedArray[currentByte+3];
        currentByte += 4;
        long[] decodedArray = new long[decodedArraySize];
        int indexInCurrentByte = 0;
        long prefixBits = 0;
        byte prefix = encodedArray[currentByte++];
        /* Read prefix */
        for (int i = prefix - 1; i >= 0; i--) {
            long bitValue = BitOperationsHelper.getBitValue(encodedArray[currentByte], indexInCurrentByte);
            prefixBits = BitOperationsHelper.setBitValue(prefixBits, i, bitValue);
            indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
            currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
        }

        prefixBits = prefixBits << (64 - prefix);

        for (int i = 0; i < decodedArraySize; i++) {
            decodedArray[i] = prefixBits;
            for (int j = 63 - prefix; j >= 0; j--) {
                long bitValue = BitOperationsHelper.getBitValue(encodedArray[currentByte], indexInCurrentByte);
                decodedArray[i] = BitOperationsHelper.setBitValue(decodedArray[i], j, bitValue);
                indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
                currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
            }
        }

        return decodedArray;
    }

    private static int increaseIndexInCurrentByte(int indexInCurrentByte) {
        return (indexInCurrentByte == 7) ? 0 : indexInCurrentByte + 1;
    }

    private static int updateCurrentByte(int indexInCurrentByte, int currentByte) {
        return (indexInCurrentByte == 0) ? currentByte + 1 : currentByte;
    }
}
