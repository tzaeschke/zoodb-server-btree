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

import java.util.NoSuchElementException;

import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;


/**
 * @author Tilmann Zaeschke
 */
public class PagedLongLong extends AbstractPagedIndex implements LongLongIndex {
	
	private transient LLIndexPage root;
	
	/**
	 * Constructor for creating new index.
	 * @param dataType Page type 
	 * @param file The file
	 */
	public PagedLongLong(PAGE_TYPE dataType, IOResourceProvider file) {
		super(file, true, 8, 8, false, dataType);
		//bootstrap index
		root = createPage(null, false);
	}

	/**
	 * Constructor for reading index from disk.
	 * @param dataType Page type 
	 * @param file The file
	 * @param pageId The ID of the root page
	 */
	public PagedLongLong(PAGE_TYPE dataType, IOResourceProvider file, int pageId) {
		super(file, true, 8, 8, false, dataType);
		root = (LLIndexPage) readRoot(pageId);
	}

	@Override
	public void insertLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, true);
		page.insert(key, value);
	}

	@Override
	public boolean insertLongIfNotSet(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, true);
		if (page.binarySearch(0, page.getNKeys(), key, value) >= 0) {
			return false;
		}
		page.insert(key, value);
		return true;
	}

	@Override
	public long removeLong(long key, long value) {
		LLIndexPage page = getRoot().locatePageForKey(key, value, false);
		if (page == null) {
			throw new NoSuchElementException("key not found: " + key + " / " + value);
		}
		return page.remove(key, value);
	}

	@Override
	LLIndexPage createPage(AbstractIndexPage parent, boolean isLeaf) {
		return new LLIndexPage(this, (LLIndexPage) parent, isLeaf);
	}

	@Override
	protected LLIndexPage getRoot() {
		return root;
	}

	@Override
	public LLEntryIterator iterator(long min, long max) {
		return new LLIterator(this, min, max);
	}

	@Override
	public LLEntryIterator iterator() {
		return new LLIterator(this, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	@Override
	protected void updateRoot(AbstractIndexPage newRoot) {
		root = (LLIndexPage) newRoot;
	}
	
	@Override
	public void print() {
		root.print("");
	}

	@Override
	public long getMinKey() {
		return root.getMinKey();
	}

	@Override
	public long getMaxKey() {
		return root.getMax();
	}

	@Override
	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator(long max, long min) {
		AbstractPageIterator<LongLongIndex.LLEntry> iter = new LLDescendingIterator(this, max, min);
		return iter;
	}

	@Override
	public AbstractPageIterator<LongLongIndex.LLEntry> descendingIterator() {
		AbstractPageIterator<LongLongIndex.LLEntry> iter = new LLDescendingIterator(this, 
				Long.MAX_VALUE, Long.MIN_VALUE);
		return iter;
	}

	@Override
	public long size() {
		throw new UnsupportedOperationException();
	}
}
