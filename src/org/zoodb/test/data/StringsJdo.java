/* 
This file is part of the PolePosition database benchmark
http://www.polepos.org

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public
License along with this program; if not, write to the Free
Software Foundation, Inc., 59 Temple Place - Suite 330, Boston,
MA  02111-1307, USA. */

package org.zoodb.test.data;

import javax.jdo.PersistenceManager;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zoodb.jdo.api.ZooSchema;
import org.zoodb.test.util.TestTools;

/**
 * @author Christian Ernst
 */
public class StringsJdo extends JdoDriver {
    
//    # strings
//    #
//    # [objects]: number of objects to be written, read and deleted
//    # [commitintervall]: when to perform an intermediate commit during write and delete
//
//    strings.objects=10000,30000,100000
//    strings.commitinterval=1000,1000,1000

    private int objects;
    private int commitInterval;

    @BeforeClass
    public static void beforeClass() {
        TestTools.createDb();
//      TestTools.defineSchema(JdoIndexedObject.class);
//      PersistenceManager pm = TestTools.openPM();
//      pm.currentTransaction().begin();
//      Schema.locate(pm, JdoIndexedObject.class).defineIndex("_int", false);
//      Schema.locate(pm, JdoIndexedObject.class).defineIndex("_string", false);
//      pm.currentTransaction().commit();
//      TestTools.closePM();

//        TestTools.defineSchema(JB0.class, JB1.class, JB2.class, JB3.class, JB4.class);
//        
//        TestTools.defineSchema(ComplexHolder0.class, ComplexHolder1.class, 
//                ComplexHolder2.class, ComplexHolder3.class, ComplexHolder4.class);
//        TestTools.defineSchema(InheritanceHierarchy0.class, InheritanceHierarchy1.class,
//                InheritanceHierarchy2.class, InheritanceHierarchy3.class, InheritanceHierarchy4.class);

//        TestTools.defineSchema(JdoIndexedObject.class, JdoIndexedPilot.class, 
//                JdoLightObject.class, JdoListHolder.class, JdoPilot.class, JdoTree.class,
//                ListHolder.class, JN1.class);
        TestTools.defineSchema(JN1.class);
        
        PersistenceManager pm = TestTools.openPM();
        pm.currentTransaction().begin();
        
//        ZooSchema.locateClass(pm, ComplexHolder2.class).defineIndex("i2", false);
//        ZooSchema.locateClass(pm, InheritanceHierarchy2.class).defineIndex("i2", false);
//        ZooSchema.locateClass(pm, JdoIndexedObject.class).defineIndex("_int", false);
//        ZooSchema.locateClass(pm, JdoIndexedObject.class).defineIndex("_string", false);
//        ZooSchema.locateClass(pm, ListHolder.class).defineIndex("_id", false);
//        ZooSchema.locateClass(pm, ListHolder.class).defineIndex("_name", false);
//        ZooSchema.locateClass(pm, JB2.class).defineIndex("b2", false);
//        ZooSchema.locateClass(pm, JdoIndexedPilot.class).defineIndex("mName", false);
//        ZooSchema.locateClass(pm, JdoIndexedPilot.class).defineIndex("mLicenseID", false);
        
        pm.currentTransaction().commit();
        TestTools.closePM();
    }

    @Test
    public void test() {
//        open();
//        close("nix");
//        open();
//        int N = 100000 + 30000 + 10000;
//        JN1[] x = new JN1[N];
//        for ( int i = 1; i <= N; i++ ){
//            x[i-1] = JN1.generate(i);
//        }
//        close("trans-");
//        System.out.println(x[0].s0);
        
        
        run(10000, 1000);
        run(30000, 1000);
        run(100000, 1000);
    }
    
    private void run(int objects, int commitInterval) {
        this.objects = objects;
        this.commitInterval = commitInterval;
        open();
        write();
        close("wrt-");
        
        open();
        read();
        close("read-");
    }

    long t1;
    private void open() {
        t1 = System.currentTimeMillis();
        prepare(TestTools.openPM());
    }
    
    private void close(String pre) {
//        System.out.println("Mem-tot: " + Runtime.getRuntime().totalMemory());
//        System.out.println("Mem-max: " + Runtime.getRuntime().maxMemory());
//        System.out.println("Mem-fre: " + Runtime.getRuntime().freeMemory());
        long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("Mem-: " + mem);
        System.out.println("Mem-x: " + mem/140000);
//        System.gc(); System.gc(); System.gc(); System.gc();
//        System.gc(); System.gc(); System.gc(); System.gc();
//        System.gc(); System.gc(); System.gc(); System.gc();
//        System.gc(); System.gc(); System.gc(); System.gc();
//        long mem2= Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//        System.out.println("Mem2-: " + mem2);
        closeDatabase();
        System.out.println(pre + "t= " + (System.currentTimeMillis()-t1));
//        TestTools.closePM();
        
        //Mem usage should be around 140.000 * JN1 = 140.000 + ~60(100?)bytes
        //Each JN1 has 10 references to the SAME String (40(base)+20*2(str)=60 bytes)!
        Assert.assertTrue("mem usage: ", mem < 50*1000*1000);
    }

    
    public void write(){
        
        int numobjects = this.objects;//setup().getObjectCount();
        int commitinterval  = this.commitInterval;//setup().getCommitInterval();
        int commitctr = 0;
        
        begin();
        for ( int i = 1; i <= numobjects; i++ ){
            store(JN1.generate(i));
            
            if ( commitinterval > 0  &&  ++commitctr >= commitinterval ){
                commitctr = 0;
                commit();
                begin();
            }
            
            addToCheckSum(i);
        }
        commit();
    }

    public void read(){
    	readExtent(JN1.class);
    }

}
