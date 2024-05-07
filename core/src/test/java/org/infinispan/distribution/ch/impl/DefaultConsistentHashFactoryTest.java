package org.infinispan.distribution.ch.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

/**
 * Test the even distribution and number of moved segments after rebalance for {@link DefaultConsistentHashFactory}
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "unit", testName = "distribution.ch.DefaultConsistentHashFactoryTest")
public class DefaultConsistentHashFactoryTest extends AbstractConsistentHashTest {
   public static final int[] NUM_SEGMENTS = new int[]{1, 2, 4, 8, 16, 60, 256, 512};
   public static final int[] NUM_NODES = new int[]{1, 2, 3, 4, 7, 10, 100};
   public static final int[] NUM_OWNERS = new int[]{1, 2, 3, 5};
   // Since the number of nodes changes, the capacity factors are repeated
   public static final float[][] CAPACITY_FACTORS = new float[][]{{1}, {2}, {1, 100}, {2, 0, 1}};
   // each element in the array is a pair of numbers: the first is the number of nodes to add
   // the second is the number of nodes to remove (the index of the removed nodes are pseudo-random)
   public static final int[][] NODE_CHANGES =
         {{1, 0}, {2, 0}, {0, 1}, {0, 2}, {2, 1}, {1, 2}, {10, 0}, {0, 10}};

   @Override
   protected int[][] nodeChanges() {
      return NODE_CHANGES;
   }

   protected ConsistentHashFactory<DefaultConsistentHash> createConsistentHashFactory() {
      return new DefaultConsistentHashFactory();
   }

   public void testConsistentHashDistribution() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();

      for (int nn : NUM_NODES) {
         List<Address> nodes = new ArrayList<>(nn);
         for (int j = 0; j < nn; j++) {
            nodes.add(new TestAddress(j, "TA"));
         }

         for (int ns : NUM_SEGMENTS) {
            if (nn < ns) {
               for (int no : NUM_OWNERS) {
                  for (float[] lf : CAPACITY_FACTORS) {
                     Map<Address, Float> lfMap = null;
                     if (lf != null) {
                        lfMap = new HashMap<>();
                        for (int i = 0; i < nn; i++) {
                           lfMap.put(nodes.get(i), lf[i % lf.length]);
                        }
                     }
                     testConsistentHashModifications(chf, nodes, ns, no, lfMap);
                  }
               }
            }
         }
      }
   }

   public void testNullCapacityFactors() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      TestAddress A = new TestAddress(0, "A");
      TestAddress B = new TestAddress(1, "B");
      TestAddress C = new TestAddress(2, "C");
      TestAddress D = new TestAddress(3, "D");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 1f);
      cf.put(C, 1f);
      cf.put(D, 1f);

      DefaultConsistentHash ch1 = chf.create(2, 60, Arrays.asList(A), cf);
      DefaultConsistentHash ch1NoCF = chf.create(2, 60, Arrays.asList(A), null);
      assertEquals(ch1, ch1NoCF);

      DefaultConsistentHash ch2 = chf.updateMembers(ch1, Arrays.asList(A, B), cf);
      ch2 = chf.rebalance(ch2);
      DefaultConsistentHash ch2NoCF = chf.updateMembers(ch1, Arrays.asList(A, B), null);
      ch2NoCF = chf.rebalance(ch2NoCF);
      assertEquals(ch2, ch2NoCF);

      DefaultConsistentHash ch3 = chf.updateMembers(ch2, Arrays.asList(A, B, C), cf);
      ch3 = chf.rebalance(ch3);
      DefaultConsistentHash ch3NoCF = chf.updateMembers(ch2, Arrays.asList(A, B, C), null);
      ch3NoCF = chf.rebalance(ch3NoCF);
      assertEquals(ch3, ch3NoCF);

      DefaultConsistentHash ch4 = chf.updateMembers(ch3, Arrays.asList(A, B, C, D), cf);
      ch4 = chf.rebalance(ch4);
      DefaultConsistentHash ch4NoCF = chf.updateMembers(ch3, Arrays.asList(A, B, C, D), null);
      ch4NoCF = chf.rebalance(ch4NoCF);
      assertEquals(ch4, ch4NoCF);
   }

   public void testDifferentCapacityFactors() {
      ConsistentHashFactory<DefaultConsistentHash> chf = createConsistentHashFactory();
      TestAddress A = new TestAddress(0, "A");
      TestAddress B = new TestAddress(1, "B");
      TestAddress C = new TestAddress(2, "C");
      TestAddress D = new TestAddress(3, "D");
      Map<Address, Float> cf = new HashMap<>();
      cf.put(A, 1f);
      cf.put(B, 1f);
      cf.put(C, 1f);
      cf.put(D, 100f);

      DefaultConsistentHash ch1 = chf.create(2, 60, Arrays.asList(A), cf);
      checkDistribution(ch1, cf);

      DefaultConsistentHash ch2 = chf.updateMembers(ch1, Arrays.asList(A, B), cf);
      ch2 = chf.rebalance(ch2);
      checkDistribution(ch2, cf);

      DefaultConsistentHash ch3 = chf.updateMembers(ch2, Arrays.asList(A, B, C), cf);
      ch3 = chf.rebalance(ch3);
      checkDistribution(ch3, cf);

      DefaultConsistentHash ch4 = chf.updateMembers(ch3, Arrays.asList(A, B, C, D), cf);
      ch4 = chf.rebalance(ch4);
      checkDistribution(ch4, cf);
   }
}
