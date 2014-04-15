package org.zoodb.test.index2.btree;

import org.junit.Test;
import org.zoodb.internal.server.index.btree.prefix.PrefixSharingHelper;

public class TestPrefixSharing {

    @Test
    public void testComputePrefix() {
        long[] arr = { 1114, 11116};

        long prefix = PrefixSharingHelper.computePrefix(arr);
        System.out.println(prefix);
    }

}
