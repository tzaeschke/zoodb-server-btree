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
package org.zoodb.test.index2.btree;

import java.util.ArrayList;
import java.util.List;

import org.zoodb.internal.server.index.btree.BTree;
import org.zoodb.internal.server.index.btree.BTreeBufferManager;
import org.zoodb.internal.server.index.btree.BTreeNode;
import org.zoodb.internal.server.index.btree.BTreeNodeFactory;
import org.zoodb.internal.server.index.btree.nonunique.NonUniquePagedBTree;
import org.zoodb.internal.server.index.btree.unique.UniquePagedBTree;
import org.zoodb.internal.util.Pair;

/**
 * Convenience class to build a UniqueBTree layer by layer.
 */
public class BTreeFactory {

    private BTree tree;
    private List<BTreeNode> prevLayer;
    private BTreeNodeFactory nodeFactory;
    private BTreeBufferManager bufferManager;
    private boolean unique = true;

    public BTreeFactory(BTreeBufferManager bufferManager)  {
        this.tree = new UniquePagedBTree(bufferManager.getPageSize(), bufferManager);
        this.nodeFactory = tree.getNodeFactory();
        this.bufferManager = bufferManager;
    }

    public BTreeFactory(BTreeBufferManager bufferManager, boolean unique)  {
        createTree(bufferManager.getPageSize(), bufferManager, unique);
        this.nodeFactory = tree.getNodeFactory();
        this.bufferManager = bufferManager;
    }

    public void addInnerLayer(List<List<Long>> nodeKeys) {
        this.addLayer(false, nodeKeys);
    }


    public void addLeafLayerDefault(List<List<Long>> nodeKeys) {
        // value is same as key
        this.addLeafLayer(zip(nodeKeys,nodeKeys));
    }

    public void addLeafLayer(List<List<Pair<Long,Long>>> nodeKeysValues) {
        List<List<Long>> nodeKeys = splitList(true, nodeKeysValues);
        splitList(true, nodeKeysValues);
        this.addLayer(true,nodeKeys);
        List<List<Long>> nodeValues = splitList(false, nodeKeysValues);;
        for(int i=0; i<prevLayer.size(); i++) {
            List<Long> values = nodeValues.get(i);
            prevLayer.get(i).setValues(padLongArray(toPrimitives(
                    values.toArray(new Long[values.size()])),
                    prevLayer.get(0).computeMaxPossibleEntries()));
        }
    }

    public void addLayer(boolean isLeaf, List<List<Long>> nodeKeys) {
        if(this.tree.isEmpty()) {
            BTreeNode root = nodeFactory.newUniqueNode(this.tree.getPageSize(), isLeaf, true);
            root.setNumKeys(nodeKeys.get(0).size());
            List<Long> keys = nodeKeys.get(0);
            root.setKeys(padLongArray(toPrimitives(
                    keys.toArray(new Long[keys.size()])),
                    root.computeMaxPossibleEntries()));
            root.recomputeSize();
            tree.getRoot().close();
            tree.setRoot(root);
            prevLayer = new ArrayList<>();
            prevLayer.add(root);
        } else {
            int indexLayer = 0;
            List<BTreeNode> newLayer = new ArrayList<BTreeNode>();
            for(BTreeNode parent : prevLayer) {
                BTreeNode[] children = new BTreeNode[parent.getNumKeys()+1];
                for(int ik = 0; ik < parent.getNumKeys()+1; ik++) {
                    BTreeNode node = nodeFactory.newUniqueNode(tree.getPageSize(), isLeaf, false);
                    List<Long> keys = nodeKeys.get(indexLayer);
                    node.setKeys(padLongArray(toPrimitives(
                            keys.toArray(new Long[keys.size()])),
                            node.computeMaxPossibleEntries()));
                    node.setNumKeys(keys.size());
                    node.recomputeSize();
                    children[ik] = node;
                    newLayer.add(node);
                    indexLayer++;
                }
                parent.setChildren(padChildrenArray(children, parent.computeMaxPossibleEntries()+1));
                parent.recomputeSize();
            }
            this.prevLayer = newLayer;
        }
    }

    public BTree getTree() {
        return this.tree;
    }

    public void clear() {
        createTree(tree.getPageSize(), bufferManager, unique);
    }

    private static List<List<Pair<Long,Long>>> zip(List<List<Long>> l1, List<List<Long>> l2) {
        List<List<Pair<Long,Long>>> ret = new ArrayList<List<Pair<Long,Long>>>();
        for(int i = 0; i< Math.min(l1.size(), l2.size()); i++) {
            List<Long> l1inner = l1.get(i);
            List<Long> l2inner = l2.get(i);
            List<Pair<Long,Long>> tmp = new ArrayList<Pair<Long,Long>>();
            for(int j = 0; j< Math.min(l1inner.size(), l2inner.size()); j++) {
                tmp.add(new Pair<Long,Long>(l1inner.get(j), l2inner.get(j)));
            }
            ret.add(tmp);
        }
        return ret;
    }

    private static List<List<Long>> splitList(boolean first, List<List<Pair<Long,Long>>> list) {
        List<List<Long>> ret = new ArrayList<List<Long>>();
        for(List<Pair<Long,Long>> outer : list) {
            ArrayList<Long> tmp = new ArrayList<Long>();
            for(Pair<Long,Long> inner : outer) {
                tmp.add(first ? inner.getA() : inner.getB());
            }
            ret.add(tmp);
        }
        return ret;
    }

    private static long[] padLongArray(long[] keys, int size) {
        long[] paddedKeyArray = new long[size - 1];
        for (int i = 0; i < keys.length; i++) {
            paddedKeyArray[i] = keys[i];
        }
        for (int i = keys.length; i < size - 1; i++) {
            paddedKeyArray[i] = 0;
        }
        return paddedKeyArray;
    }

    private static BTreeNode[] padChildrenArray(BTreeNode[] children, int size) {
        BTreeNode[] paddedNodeArray = new BTreeNode[size];
        for (int i = 0; i < children.length; i++) {
            paddedNodeArray[i] = children[i];
        }
        for (int i = children.length; i < size; i++) {
            paddedNodeArray[i] = null;
        }
        return paddedNodeArray;
    }

    public static long[] toPrimitives(Long... objects) {
        long[] primitives = new long[objects.length];
        for (int i = 0; i < objects.length; i++)
            primitives[i] = objects[i];

        return primitives;
    }

    private void createTree(int pageSize, BTreeBufferManager bufferManager, boolean unique) {
        if (unique) {
            this.tree = new UniquePagedBTree(pageSize, bufferManager);
        } else {
            this.tree = new NonUniquePagedBTree(pageSize, bufferManager);
        }
    }

}
