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
package org.zoodb.internal.util;

/**
 * A fast stack implementation.
 * 
 * @author ztilmann
 *
 * @param <T>
 */
public class FastStack<T> {

	private static final int INC = 10;
	
	@SuppressWarnings("unchecked")
	private T[] data = (T[]) new Object[10];
	private int n = 0;
	
	@SuppressWarnings("unchecked")
	public void push(T obj) {
		if (n+1 >= data.length) {
			T[] data2 = (T[]) new Object[data.length+INC];
			System.arraycopy(data, 0, data2, 0, data.length);
			data = data2;
		}
		data[n++] = obj;
	}
	
	public T pop() {
		return data[--n];
	}
	
	public T peek() {
		return data[n-1];
	}
	
	public int size() {
		return n;
	}
}
