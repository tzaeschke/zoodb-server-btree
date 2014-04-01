package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.unique.UniqueBTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeHashBufferManager;

import java.util.LinkedHashMap;
import java.util.Map;

public class TestUnique {

    private BTreeBufferManager bufferManager = new BTreeHashBufferManager();

    @Test
    public void testInsertDuplicates() {
        int order = 5;
        UniqueBTree tree = factory(order).getTree();

        Map<Long, Long> entries = new LinkedHashMap<Long, Long>() {{
            put(1L, 1L);
            put(2L, 2L);
        }};

        for (Map.Entry<Long, Long> entry : entries.entrySet()) {
            tree.insert(entry.getKey(), entry.getValue());
        }
        for (int i = 0; i < 50; i++) {
            tree.insert(5L, Long.valueOf(i));
        }
        for (int i = 0; i < 10; i++) {
            System.out.println(tree.search(5L));
            tree.delete(5L);
        }
        System.out.println(tree.search(5L));
        System.out.println(tree);
    }

    @Test(expected = IllegalStateException.class)
    public void testSameKeyPair() {
        int order = 5;
        UniqueBTree tree = factory(order).getTree();
        tree.insert(1, 1);
        tree.insert(1, 2);
    }



    private BTreeFactory factory(int order) {
        return new BTreeFactory(order, bufferManager);
    }

}
