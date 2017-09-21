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
package org.zoodb.test.index2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Scanner;

import org.junit.Test;
import org.zoodb.internal.server.DiskIO.PAGE_TYPE;
import org.zoodb.internal.server.IOResourceProvider;
import org.zoodb.internal.server.StorageRootInMemory;
import org.zoodb.internal.server.index.IndexFactory;
import org.zoodb.internal.server.index.LongLongIndex;
import org.zoodb.internal.server.index.LongLongIndex.LLEntry;
import org.zoodb.internal.server.index.LongLongIndex.LLEntryIterator;
import org.zoodb.tools.ZooConfig;


/**
 * @author Tilmann Zaeschke
 *
 */
public class TestBTree_ParentId {

    
    @Test
    public void testIndex() {
		IOResourceProvider paf = new StorageRootInMemory(ZooConfig.getFilePageSize()).createChannel();
        LongLongIndex idx = IndexFactory.createIndex(PAGE_TYPE.FIELD_INDEX, paf);
       
        long[] keys = loadData();
        long[] vals = new long[keys.length];
        for (int i = 0; i < vals.length; i++) {
        	vals[i] = i+100;  //simulate OIDs
        }
       
        for (int i = 0; i < vals.length; i++) {
        	idx.insertLong(keys[i], vals[i]);
        }

        int pageId = paf.writeIndex(idx::write);
        idx = IndexFactory.loadIndex(PAGE_TYPE.FIELD_INDEX, paf, pageId);
        
        for (int i = 0; i < vals.length; i++) {
        	LLEntryIterator it = idx.iterator(keys[i], keys[i]);
        	boolean found = false;
        	while (it.hasNext()) {
        		LLEntry e = it.next();
        		assertEquals(keys[i], e.getKey());
        		if (e.getValue() == vals[i]) {
        			found = true;
        		}
        	}
        	if (!found) {
        		fail("i=" + i);
        	}
        }
    }
    
    
	private long[] loadData() {
		//return I;
		
		InputStream is = TestBTree_ParentId.class.getResourceAsStream("parentId.txt");
		if (is==null) {
			throw new NullPointerException();
		}
		Scanner s = new Scanner(is);
		s.useDelimiter(",");
		ArrayList<Long> ret = new ArrayList<Long>();
		while (s.hasNext()) {
			ret.add(s.nextLong());
		}
		s.close();
		try {
			is.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		System.out.println("reading: " + ret.size());
		long[] ret2 = new long[ret.size()];
		for (int i = 0; i < ret.size(); i++) {
			ret2[i] = ret.get(i);
		}
		return ret2;
	}

}
