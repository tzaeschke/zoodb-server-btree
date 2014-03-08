package org.zoodb.test.index2.btree;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import org.zoodb.internal.server.index.btree.BTreeNode;

public class TestNode {
	
	@Test
	public void putLeaf() {
        BTreeNode leafNode = new BTreeNode(null, 6, true);
        assertEquals(leafNode.getValues().length, 5); 
        assertEquals(leafNode.getKeys().length, 5); 

        leafNode.put(3, 3);
        assertEquals(leafNode.getNumKeys(), 1);
        assertEquals(leafNode.getKeys()[0], 3);
        leafNode.put(1, 5);
        assertEquals(leafNode.getNumKeys(), 2);
        assertEquals(leafNode.getKeys()[0], 1);
        assertEquals(leafNode.getKeys()[1], 3);
        leafNode.put(5, 1);
        assertEquals(leafNode.getNumKeys(), 3);
        assertEquals(leafNode.getKeys()[0], 1);
        assertEquals(leafNode.getKeys()[1], 3);
        assertEquals(leafNode.getKeys()[2], 5);
        leafNode.put(4, 2);
        assertEquals(leafNode.getNumKeys(), 4);
        assertEquals(leafNode.getKeys()[0], 1);
        assertEquals(leafNode.getKeys()[1], 3);
        assertEquals(leafNode.getKeys()[2], 4);
        assertEquals(leafNode.getKeys()[3], 5);
        leafNode.put(2, 4);
        assertEquals(leafNode.getNumKeys(), 5);
        assertEquals(leafNode.getKeys()[0], 1);
        assertEquals(leafNode.getKeys()[1], 2);
        assertEquals(leafNode.getKeys()[2], 3);
        assertEquals(leafNode.getKeys()[3], 4);
        assertEquals(leafNode.getKeys()[4], 5);
        
        assertEquals(leafNode.getValues()[0], 5);
        assertEquals(leafNode.getValues()[1], 4);
        assertEquals(leafNode.getValues()[2], 3);
        assertEquals(leafNode.getValues()[3], 2);
        assertEquals(leafNode.getValues()[4], 1);
	}
	
	@Test
	public void innerNodePut() {
		final int order = 6;
        BTreeNode innerNode = new BTreeNode(null, order, false);
        assertEquals(5, innerNode.getKeys().length); 
        assertEquals(order, innerNode.getChildren().length); 

        BTreeNode child1 = new BTreeNode(null, order, true);
        child1.put(1, 1);
        BTreeNode child2 = new BTreeNode(null, order, true);
        child2.put(2, 2);
        BTreeNode child3 = new BTreeNode(null, order, true);
        child3.put(3, 3);
        BTreeNode child4 = new BTreeNode(null, order, true);
        child4.put(4, 4);
        BTreeNode child5 = new BTreeNode(null, order, true);
        child5.put(5, 5);
        BTreeNode child6 = new BTreeNode(null, order, true);
        child6.put(6, 6);
        
        innerNode.put(3, child1, child4);
        assertEquals(1, innerNode.getNumKeys());
        assertEquals(3, innerNode.getKeys()[0]);
        assertEquals(child1, innerNode.getChildren()[0]);
        assertEquals(child4, innerNode.getChildren()[1]);
        
        innerNode.put(1, child2);
        assertEquals(2, innerNode.getNumKeys());
        assertEquals(1, innerNode.getKeys()[0]);
        assertEquals(3, innerNode.getKeys()[1]);
        assertEquals(child1, innerNode.getChildren()[0]);
        assertEquals(child2, innerNode.getChildren()[1]);
        assertEquals(child4, innerNode.getChildren()[2]);
        
        innerNode.put(5, child6);
        assertEquals(3, innerNode.getNumKeys());
        assertEquals(1, innerNode.getKeys()[0]);
        assertEquals(3, innerNode.getKeys()[1]);
        assertEquals(5, innerNode.getKeys()[2]);
        assertEquals(child1, innerNode.getChildren()[0]);
        assertEquals(child2, innerNode.getChildren()[1]);
        assertEquals(child4, innerNode.getChildren()[2]);
        assertEquals(child6, innerNode.getChildren()[3]);
        
        innerNode.put(2, child3);
        assertEquals(4, innerNode.getNumKeys());
        assertEquals(1, innerNode.getKeys()[0]);
        assertEquals(2, innerNode.getKeys()[1]);
        assertEquals(3, innerNode.getKeys()[2]);
        assertEquals(5, innerNode.getKeys()[3]);
        assertEquals(child1, innerNode.getChildren()[0]);
        assertEquals(child2, innerNode.getChildren()[1]);
        assertEquals(child3, innerNode.getChildren()[2]);
        assertEquals(child4, innerNode.getChildren()[3]);
        assertEquals(child6, innerNode.getChildren()[4]);
        
        innerNode.put(4, child5);
        assertEquals(5, innerNode.getNumKeys());
        assertEquals(1, innerNode.getKeys()[0]);
        assertEquals(2, innerNode.getKeys()[1]);
        assertEquals(3, innerNode.getKeys()[2]);
        assertEquals(4, innerNode.getKeys()[3]);
        assertEquals(5, innerNode.getKeys()[4]);

        
        assertArrayEquals(new BTreeNode[]{child1,child2,child3,child4,child5,child6}, 
        					innerNode.getChildren());
	}
	
	

}
