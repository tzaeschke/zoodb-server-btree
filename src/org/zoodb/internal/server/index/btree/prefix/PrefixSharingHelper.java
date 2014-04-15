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
        return (first >> (64 - prefix));
    }

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
