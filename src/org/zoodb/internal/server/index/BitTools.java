/*
 * Copyright 2009-2016 Tilmann Zaeschke. All rights reserved.
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
package org.zoodb.internal.server.index;

import org.zoodb.api.impl.ZooPC;

public class BitTools {

	/** Value to recognize 'null'in indices. Using MIN_VALUE so that NULL is the lowest value
	 * when sorted. */ 
	public static final long NULL = Long.MIN_VALUE;
	public static final long EMPTY_STRING = 0L;
	
    public static final long getMinPosInPage(long pos) {
        return pos & 0xFFFFFFFF00000000L;
    }

    public static final long getMaxPosInPage(long pos) {
        return pos | 0x00000000FFFFFFFFL;
    }

    public static final int getPage(long pos) {
        return (int) (pos >> 32);
    }

    public static final int getOffs(long pos) {
        return (int)(pos & 0x00000000FFFFFFFFL);
    }

    public static final long getPos(int pageId, int offs) {
        return (((long)pageId) << 32) + offs;
    }

    /**
     * @param value The value that should be converted 
     * @return long representation.
     */
	public static long toSortableLong(double value) {
		//To create a sortable long, we convert the double to a long using the IEEE-754 standard,
		//which stores floats in the form <sign><exponent-127><mantissa> .
		//This result is properly ordered longs for all positive doubles. Negative values have
		//inverse ordering. For negative doubles, we therefore simply invert them to make them 
		//sortable, however the sign must be inverted again to stay negative.
		long r = Double.doubleToRawLongBits(value);
		return (r >= 0) ? r : r ^ 0x7FFFFFFFFFFFFFFFL;
	}

	public static long toSortableLong(float value) {
		//see toSortableLong(double)
		int r =  Float.floatToRawIntBits(value);
		return (r >= 0) ? r : r ^ 0x7FFFFFFF;
	}

	public static double toDouble(long value) {
		return Double.longBitsToDouble(value >= 0.0 ? value : value ^ 0x7FFFFFFFFFFFFFFFL);
	}

	public static float toFloat(long value) {
		int iVal = (int) value;
		return Float.intBitsToFloat(iVal >= 0.0 ? iVal : iVal ^ 0x7FFFFFFF);
	}

	public static long toSortableLong(String s) {
		if (s == null) {
			return NULL;
		}
		if (s.length() == 0) {
			return EMPTY_STRING;
		}
		
    	// store magic number: 6 chars + (hash >> 16)
		long n = 0;
    	int i = 0;
    	for ( ; i < 6 && i < s.length(); i++ ) {
    		n |= (byte) s.charAt(i);
    		n = n << 8;
    	}
    	//Fill with empty spaces if string is too short
    	for ( ; i < 6; i++) {
    		n = n << 8;
    	}
    	n = n << 8;

    	//add hashcode
    	n |= (0xFFFF & s.hashCode());
		return n;
	}

	public static long toSortableLong(ZooPC pc) {
		if (pc == null) {
			return NULL;
		}
		return pc.jdoZooGetOid();
	}
	
	/**
	 * @param prefix The String prefix
	 * @return the minimum index-key for strings with the given prefix
	 */
	public static long toSortableLongPrefixMinHash(String prefix) {
		if (prefix != null && prefix.length() == 0) {
			return Long.MIN_VALUE;
		}
		long key = toSortableLong(prefix);
		return key & ((prefix.length() < 6) 
				? ~(0xFFFFFFFFFFFFFFFFL >>> (prefix.length()*8)) 
				: 0xFFFFFFFFFFFF0000L);
	}

	/**
	 * @param prefix The String prefix
	 * @return the maximum index-key for strings with the given prefix
	 */
	public static long toSortableLongPrefixMaxHash(String prefix) {
		if (prefix != null && prefix.length() == 0) {
			return Long.MAX_VALUE;
		}
		long key = toSortableLong(prefix);
		return key | ((prefix.length() < 6) 
				? 0xFFFFFFFFFFFFFFFFL >>> (prefix.length()*8) 
				: 0x000000000000FFFFL);
	}
	
}
