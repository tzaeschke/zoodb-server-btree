package org.zoodb.internal.server.index.btree.prefix;

public class BitOperationsHelper {

    public static boolean isBitSet(long number, int position) {
        return (number & (1L << position)) != 0;
    }

    public static long getBitValue(long number, int position) {
        return isBitSet(number, position) ? 1L : 0L;
    }

    public static byte setBit(byte number, int position) {
        return (byte) (number | (1 << position));
    }

    public static byte unsetBit(byte number, int position) {
        return (byte) (number & ~(1 << position));
    }

    public static byte setBitValue(byte number, int position, long value) {
        byte returned;
        if (value == 0) {
            returned = unsetBit(number, position);
        } else {
            returned = setBit(number, position);
        }
        return returned;
    }

    public static long unsetBit(long number, int position) {
        return (number & ~(1L << position));
    }

    public static long setBit(long number, int position) {
        return (number | (1L << position));
    }

    public static long setBitValue(long number, int position, long value) {
        long returned;
        if (value == 0) {
            returned = unsetBit(number, position);
        } else {
            returned = setBit(number, position);
        }
        return returned;
    }
}
