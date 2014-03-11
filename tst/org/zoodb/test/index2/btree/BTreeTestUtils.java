package org.zoodb.test.index2.btree;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class BTreeTestUtils {

    public static Map<Long, Long> randomUniformKeyValues(int size) {
        Map<Long, Long> map = new HashMap<Long, Long>();
        Random random = new Random();
        for (long i = 0; i < size; i++) {
            map.put(i, random.nextLong());
        }
        return map;
    }
}
