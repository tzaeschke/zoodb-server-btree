package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.BTree;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestBTree {

    @Test
    public void searchSingleNode() {
        final int order = 10;
        BTree tree = new BTree(order);

        Map<Long, Long> keyValueMap = BTreeTestUtils.randomUniformKeyValues(5);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, Long> entry: keyValueMap.entrySet()) {
            long expectedValue = entry.getValue();
            long value = tree.search(entry.getKey());
            assertEquals("Incorrect value retrieved.", expectedValue, value);
        }
    }

    @Test
    public void searchAfterSplit() {
        final int order = 5;
        BTree tree = new BTree(order);

        Map<Long, Long> keyValueMap = BTreeTestUtils.randomUniformKeyValues(5);
        for (Map.Entry<Long, Long> entry : keyValueMap.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, Long> entry: keyValueMap.entrySet()) {
            long expectedValue = entry.getValue();
            long value = tree.search(entry.getKey());
            assertEquals("Incorrect value retrieved.", expectedValue, value);
        }
    }

}
