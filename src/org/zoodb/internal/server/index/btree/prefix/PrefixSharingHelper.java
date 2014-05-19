package org.zoodb.internal.server.index.btree.prefix;

import java.util.Arrays;

public class PrefixSharingHelper {

    public static final int PREFIX_SHARING_METADATA_SIZE = 5;
    public static final int STORAGE_MANAGER_METADATA_SIZE = 2;
    public static final int SMALLEST_POSSIBLE_COMPRESSION_SIZE = 8 + PREFIX_SHARING_METADATA_SIZE + STORAGE_MANAGER_METADATA_SIZE;

    /**
     * Compute the size of the bit prefix shared by the two long values.
     *
     * @param first
     * @param last
     * @return
     */
    public static long computePrefix(long first, long last) {
//        if (first == last) {
//            return 64;
//        }
//        long prefix = 0;
//        long low = 0;
//        long high = 63;
//        long mid = 0;
//        while (low <= high) {
//            mid = low + ((high - low) >> 1);
//            long firstPrefix = first >> (64 - mid);
//            long lastPrefix = last >> (64 - mid);
//            if (firstPrefix == lastPrefix) {
//                low = mid + 1;
//                prefix = mid;
//            } else {
//                high = mid - 1;
//            }
//        }
//        return prefix;
        return Long.numberOfLeadingZeros(first ^ last);
    }

    /**
     * Computes the bit prefix of the array arr. The array has to be sorted prior
     * to this operation.
     * @param arr        The array received as argument.
     * @return           The bit prefix
     */
    public static long computePrefix(long[] arr) {
        return computePrefix(arr, arr.length);
    }

    public static long computePrefix(long[] arr, int arrayLength) {
        long prefix;
        if(arrayLength > 0) {
            long first = arr[0];
            long last = arr[arrayLength - 1];
            prefix = computePrefix(first, last);
        } else {
            prefix = 64;
        }
//        System.out.println(String.format("First:\t %d\t %-72s",first, toBinaryLongString(first)));
//        System.out.println(String.format("Last:\t %d\t %-72s",last, toBinaryLongString(last)));
//        System.out.println(String.format("Prefix:\t %d\t %-72s",prefix, toBinaryLongString(first >> (64 - prefix))));
        return prefix;
    }

    public static int computeIndexForSplitAfterInsert(long[] arr, int arrayLength) {
        return computeIndexForSplitAfterInsert(arr, arrayLength, 0, 0, 0, Long.MAX_VALUE);
    }

    public static int computeIndexForSplitAfterInsert(long[] arr, int arrayLength, int header, int weightKey, int weightChild, long maxSize) {
         /*
         *  Perform a binary search by computing the sizes of the left and right array
         *  after splitting by a certain index.
         *
         *  If the left array has a larger size, move the splitting point to the right.
         */
        int low = 0 ;
        int high = arrayLength - 1;
        int mid = 0;
        int optimalIndex = 0;
        long optimalDiff = Long.MAX_VALUE;
        while (low <= high) {
            mid = low + ((high - low) >> 1);
            long prefixLeft = computePrefix(arr[0], arr[mid]);
            long prefixRight = computePrefix(arr[mid+1], arr[arrayLength - 1]);
            long sizeLeft = computeArraySize(prefixLeft, (mid + 1), header, weightKey, weightChild);
            long sizeRight = computeArraySize(prefixRight, (arrayLength - 1 - mid), header, weightKey, weightChild);
            if (optimalDiff > Math.abs(sizeLeft - sizeRight)) {
                optimalIndex = mid;
                optimalDiff = Math.abs(sizeLeft - sizeRight);
            }
            if ((sizeLeft < sizeRight) && sizeLeft <= maxSize) {
                //increase sizeLeft and decrease sizeRight
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return optimalIndex;
    }

    /**
     * Computes the optimal split point for a prefix shared array after inserting
     * a new element newElement. The optimal split point is the index that splits
     * the array into two prefix shared arrays of relatively equal size.
     *
     * @param arr               The prefix shared array.
     * @return
     */
    public static int computeIndexForSplitAfterInsert(long[] arr) {
        return computeIndexForSplitAfterInsert(arr, arr.length);
    }

    /**
     * Compute the number of keys to move from the left array to the right array.
     *
     * As a precondition, the left array should have a larger storage size than the
     * right array.
     *
     * @param first
     * @param second
     * @return
     */
    public static int computeIndexForRedistributeLeftToRight(long[] first, long[] second) {
        return computeIndexForRedistributeLeftToRight(first, first.length, second, second.length);
    }

    public static int computeIndexForRedistributeLeftToRight(long[] first, int firstArraySize,
                                                             long[] second, int secondArraySize,
                                                             int header, int weightKey, int weightChild,
                                                             int maxSize) {

        if (firstArraySize == 0) {
            return secondArraySize >> 1;
        } else if (secondArraySize == 0) {
            return firstArraySize >> 1;
        }
        /*
         *  Perform a binary search on the index in the first array that would
         *  provide the optimal split point.
         */
        int low = 0 ;
        int high = firstArraySize - 1;
        int mid = 0;
        int optimalIndex = 0;
        long optimalDiff = Long.MAX_VALUE;
        while (low < high) {
            mid = low + ((high - low) >> 1);
            long prefixLeft = computePrefix(first[0], first[mid - 1]);
            long prefixRight = computePrefix(first[mid], second[secondArraySize - 1]);
            long sizeLeft = computeArraySize(prefixLeft, mid, header, weightKey, weightChild);
            long sizeRight = computeArraySize(prefixRight, (firstArraySize - mid + secondArraySize), header, weightKey,weightChild);
            if (optimalDiff > Math.abs(sizeLeft - sizeRight)) {
                optimalIndex = mid;
                optimalDiff = Math.abs(sizeLeft - sizeRight);
            }
            if (sizeLeft > sizeRight && sizeRight < maxSize) {
                //decrease sizeLeft and increase sizeRight
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }

        long prefixLeft, prefixRight, sizeLeft, sizeRight;
        boolean done;
        do {
            done = false;
            prefixLeft = computePrefix(first[0], first[optimalIndex - 1]);
            prefixRight = computePrefix(first[optimalIndex], second[secondArraySize - 1]);
            sizeLeft = computeArraySize(prefixLeft, optimalIndex, header, weightKey, weightChild);
            sizeRight = computeArraySize(prefixRight, (firstArraySize - optimalIndex + secondArraySize), header, weightKey,weightChild);
            if (sizeLeft > maxSize) {
                optimalIndex--;
            } else if (sizeRight > maxSize) {
                optimalIndex++;
                if (optimalIndex > firstArraySize) {
                    return optimalIndex;
                }
            } else {
                done = true;
            }
        } while (done == false);

        assert sizeLeft <= maxSize;
        assert sizeRight <= maxSize;
        return optimalIndex;
    }

    public static int computeIndexForRedistributeLeftToRight(long[] first, int firstArraySize, long[] second, int secondArraySize) {
        return computeIndexForRedistributeLeftToRight(first, firstArraySize, second, secondArraySize, 0, 0, 0, Integer.MAX_VALUE);
    }

    private static long computeArraySize(long prefix, int elements, int header, int weightKey, int weightChild) {
        return header + encodedArraySize(elements, prefix) + elements * weightKey + elements * weightChild;
    }

    private static long computeArraySize(long prefix, int elements, int header, int weight) {
        return header + encodedArraySize(elements, prefix) + elements * weight;
    }

    /**
     * Compute the number of keys to move from the right array to the left array.
     *
     * As a precondition, the right array should have a larger storage size than the
     * left array.
     *
     * @param first
     * @param second
     * @return
     */
    public static int computeIndexForRedistributeRightToLeft(long[] first, long[] second) {
        return computeIndexForRedistributeRightToLeft(first, first.length, second, second.length);
    }

    public static int computeIndexForRedistributeRightToLeft(long[] first, int firstArraySize,
                                                             long[] second, int secondArraySize) {
        return computeIndexForRedistributeRightToLeft(first, firstArraySize, second, secondArraySize, 0, 0, 0, Integer.MAX_VALUE);
    }

    public static int computeIndexForRedistributeRightToLeft(long[] first, int firstArraySize,
                                                             long[] second, int secondArraySize,
                                                             int header, int weightKey, int weightChild, int maxSize) {
        if (firstArraySize == 0) {
            return secondArraySize >> 1;
        } else if (secondArraySize == 0) {
            return firstArraySize >> 1;
        }
        int low = 0 ;
        int high = secondArraySize - 1;
        int mid = 0;
        int optimalIndex = 0;
        long optimalDiff = Long.MAX_VALUE;
        while (low <= high) {
            mid = low + ((high - low) >> 1);
            long prefixLeft = computePrefix(first[0], second[mid]);
            long prefixRight = computePrefix(second[mid + 1], second[secondArraySize - 1]);
            long sizeLeft = computeArraySize(prefixLeft, (mid + 1 + firstArraySize), header, weightKey, weightChild);
            long sizeRight = computeArraySize(prefixRight, (secondArraySize - mid - 1), header, weightKey, weightChild);
            if (optimalDiff > Math.abs(sizeLeft - sizeRight)) {
                optimalIndex = mid;
                optimalDiff = Math.abs(sizeLeft - sizeRight);
            }
            if (sizeLeft < sizeRight && sizeLeft < maxSize) {
                //increase sizeLeft
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        long prefixLeft, prefixRight, sizeLeft, sizeRight;
        boolean done;
        do {
            done = false;
            prefixLeft = computePrefix(first[0], second[optimalIndex]);
            prefixRight = computePrefix(second[optimalIndex + 1], second[secondArraySize - 1]);
            sizeLeft = computeArraySize(prefixLeft, (optimalIndex + 1 + firstArraySize), header, weightKey, weightChild);
            sizeRight = computeArraySize(prefixRight, (secondArraySize - optimalIndex - 1), header, weightKey, weightChild);
            if (sizeLeft > maxSize && optimalIndex >= 0) {
                optimalIndex--;
                if (optimalIndex == -1) {
                    done = true;
                }
            } else if (sizeRight > maxSize) {
                optimalIndex++;
            } else {
                done = true;
            }
        } while (done == false);

        if (optimalIndex == 0) {
            return optimalIndex;
        }
        assert sizeLeft <= maxSize;
        assert sizeRight <= maxSize;
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

    public static byte[] encodeArray(long[] array, long prefix) {
        return encodeArray(array, array.length, prefix);
    }
    /**
     * Encode a prefix shared long array into an array of bytes.
     *
     * @param array
     * @param prefix
     * @return
     */
    public static byte[] encodeArray(long[] array, int arrayLength, long prefix) {
        int inputArrayIndex = 0;
        int currentByte = 0;
        int indexInCurrentByte = 0;

        /* Compute the number of bits to be stored */
        int outputArraySize = encodedArraySize(arrayLength, prefix);

        byte[] outputArray = new byte[outputArraySize + PREFIX_SHARING_METADATA_SIZE];

        /*Write the size of the array as an int - always 4 bytes */
        outputArray[currentByte++] = (byte) (arrayLength >>> 24);
        outputArray[currentByte++] = (byte) (arrayLength >>> 16);
        outputArray[currentByte++] = (byte) (arrayLength >>> 8);
        outputArray[currentByte++] = (byte) arrayLength;

        /* Write the prefix size */
        outputArray[currentByte++] = (byte) prefix;

        long prefixBits;
        if(arrayLength > 0) {
            prefixBits = prefixBits(prefix, array[0]);
        } else {
        	prefixBits = 0;
        }
        /* Encode the prefix*/
        for (int i = (int) (prefix - 1); i >= 0; i--) {
            long bitValue = BitOperationsHelper.getBitValue(prefixBits, i);
            outputArray[currentByte] = BitOperationsHelper.setBitValue(outputArray[currentByte], indexInCurrentByte, bitValue);
            indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
            currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
        }

        /* Perform the actual encoding */
        while (inputArrayIndex < arrayLength) {
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
    
    public static int encodedArraySize(int arraySize, long prefixLength) {
	    int bitsToStore = (int) (prefixLength + (64 - prefixLength) * arraySize);
        //int outputArraySize = (int) Math.ceil(bitsToStore/ 8.0);
        bitsToStore = bitsToStore >> 3;
        int outputArraySize = bitsToStore + (bitsToStore < 0 ? 0 : 1) >> 0;
        return outputArraySize;
    }

    /**
     * Decoded a prefix shared encoded array
     * @param encodedArray
     * @return
     */
    public static long[] decodeArray(byte[] encodedArray) {
        int currentByte = 0;
        int decodedArraySize = byteArrayToInt(encodedArray, currentByte); 
        currentByte += 4;
        byte prefix = encodedArray[currentByte++];

        return decodeArray(Arrays.copyOfRange(encodedArray, 5, encodedArray.length), decodedArraySize, prefix);
    }
    
    public static long[] decodeArray(byte[] encodedArrayWithoutMetadata, int decodedArraySize, byte prefixLength) {
    	byte[] encodedArray = encodedArrayWithoutMetadata;
        int currentByte = 0;
        long[] decodedArray = new long[decodedArraySize];
        int indexInCurrentByte = 0;
        long prefixBits = 0;
        
        /* Read prefix */
        for (int i = prefixLength - 1; i >= 0; i--) {
            long bitValue = BitOperationsHelper.getBitValue(encodedArray[currentByte], indexInCurrentByte);
            prefixBits = BitOperationsHelper.setBitValue(prefixBits, i, bitValue);
            indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
            currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
        }

        prefixBits = prefixBits << (64 - prefixLength);

        for (int i = 0; i < decodedArraySize; i++) {
            decodedArray[i] = prefixBits;
            for (int j = 63 - prefixLength; j >= 0; j--) {
                long bitValue = BitOperationsHelper.getBitValue(encodedArray[currentByte], indexInCurrentByte);
                decodedArray[i] = BitOperationsHelper.setBitValue(decodedArray[i], j, bitValue);
                indexInCurrentByte = increaseIndexInCurrentByte(indexInCurrentByte);
                currentByte = updateCurrentByte(indexInCurrentByte, currentByte);
            }
        }

        return decodedArray;
    }
    
    public static int byteArrayToInt(byte[] array, int indexInArray) {
        return 	( array[indexInArray] << 24 )  |
                ( (array[indexInArray+1] & 0xFF) << 16 )  |
                ( (array[indexInArray+2] & 0xFF) << 8 )   |
                    ( array[indexInArray+3] & 0xFF );
    }

    private static int increaseIndexInCurrentByte(int indexInCurrentByte) {
        return (indexInCurrentByte == 7) ? 0 : indexInCurrentByte + 1;
    }

    private static int updateCurrentByte(int indexInCurrentByte, int currentByte) {
        return (indexInCurrentByte == 0) ? currentByte + 1 : currentByte;
    }
}
