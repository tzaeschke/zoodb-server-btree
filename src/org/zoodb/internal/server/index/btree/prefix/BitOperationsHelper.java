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
package org.zoodb.internal.server.index.btree.prefix;

/**
 * Contains helper methods for bit operations used when encoding/decoding
 * arrays based on the binary prefix.
 *
 * @author Jonas Nick
 * @author Bogdan Vancea
 */
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
