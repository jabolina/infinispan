package org.infinispan.distribution.ch.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "distribution.ch.DefaultConsistentHashUnitTest")
public class DefaultConsistentHashUnitTest extends AbstractConsistentHashTest {

   @Override
   protected int[][] nodeChanges() {
      throw new IllegalStateException("Should not run");
   }

   public void testHashCreationEquivalence() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> members = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));

      Map<Address, Float> capacityFactors = new HashMap<>();
      capacityFactors.put(members.get(0), 1.0f);
      capacityFactors.put(members.get(1), 1.0f);
      capacityFactors.put(members.get(2), 1.0f);

      DefaultConsistentHash dch1 = chf.create(2, 256, members, capacityFactors);
      DefaultConsistentHash dch2 = chf.create(2, 256, members, capacityFactors);

      assertIsEquivalent(dch1, dch2);
   }

   public void testHashEquivalenceOrder() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> ch1 = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));
      List<Address> ch2 = List.of(new TestAddress(0, "TA"), new TestAddress(2, "TA"), new TestAddress(1, "TA"));


      DefaultConsistentHash dch1 = chf.create(2, 256, ch1, capacityFactors(ch1));
      DefaultConsistentHash dch2 = chf.create(2, 256, ch2, capacityFactors(ch2));

      assertIsEquivalent(dch1, dch2);
   }

   public void testHashEquivalenceAfterLeaver() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> twoMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"));
      List<Address> threeMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));


      DefaultConsistentHash dch1 = chf.create(2, 256, twoMembers, capacityFactors(twoMembers));
      DefaultConsistentHash dch2 = chf.create(2, 256, threeMembers, capacityFactors(threeMembers));

      DefaultConsistentHash after = rebalanceIteration(chf, dch2, 0, 1, twoMembers, capacityFactors(twoMembers));
      assertIsEquivalent(dch1, after);
   }

   public void testHashEquivalenceAfterJoiner() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> twoMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"));
      List<Address> threeMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));

      Map<Address, Float> capacityFactors = new HashMap<>();
      capacityFactors.put(twoMembers.get(0), 1.0f);
      capacityFactors.put(twoMembers.get(1), 1.0f);
      capacityFactors.put(threeMembers.get(2), 1.0f);

      DefaultConsistentHash dch1 = chf.create(2, 256, twoMembers, capacityFactors);
      DefaultConsistentHash dch2 = chf.create(2, 256, threeMembers, capacityFactors);

      DefaultConsistentHash after = rebalanceIteration(chf, dch1, 1, 0, threeMembers, capacityFactors);
      assertIsEquivalent(dch2, after);
   }

   public void testHashEquivalenceAfterRestart() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> twoMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"));
      List<Address> threeMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));

      Map<Address, Float> capacityFactors = new HashMap<>();
      capacityFactors.put(twoMembers.get(0), 1.0f);
      capacityFactors.put(twoMembers.get(1), 1.0f);
      capacityFactors.put(threeMembers.get(2), 1.0f);

      DefaultConsistentHash dch1 = chf.create(2, 256, twoMembers, capacityFactors);
      DefaultConsistentHash dch2 = transition(chf, threeMembers);

      DefaultConsistentHash left = rebalanceIteration(chf, dch2, 0, 1, twoMembers, capacityFactors(twoMembers));

      // FIXME: Should be equivalent.
      assertIsEquivalent(dch1, left);

      DefaultConsistentHash joined = rebalanceIteration(chf, left, 1, 0, threeMembers, capacityFactors);
      assertIsEquivalent(dch2, joined);
   }

   public void testHashEquivalenceAfterTransition() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> threeMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));

      Map<Address, Float> capacityFactors = new HashMap<>();
      capacityFactors.put(threeMembers.get(0), 1.0f);
      capacityFactors.put(threeMembers.get(1), 1.0f);
      capacityFactors.put(threeMembers.get(2), 1.0f);

      DefaultConsistentHash complete = chf.create(2, 256, threeMembers, capacityFactors);
      DefaultConsistentHash other = transition(chf, threeMembers);

      assertIsEquivalent(complete, other);
   }

   public void testEquivalenceAfterTransitions() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> threeMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));

      DefaultConsistentHash first = transition(chf, threeMembers);
      DefaultConsistentHash second = transition(chf, threeMembers);

      assertIsEquivalent(first, second);
   }

   public void testRestartEquivalenceAfterTransitions() {
      ConsistentHashFactory<DefaultConsistentHash> chf = new DefaultConsistentHashFactory();

      List<Address> threeMembers = List.of(new TestAddress(0, "TA"), new TestAddress(1, "TA"), new TestAddress(2, "TA"));

      DefaultConsistentHash original = transition(chf, threeMembers);
      DefaultConsistentHash left = rebalanceIteration(chf, original, 0, 1, threeMembers.subList(0, 2), capacityFactors(threeMembers.subList(0, 2)));
      DefaultConsistentHash restarted = rebalanceIteration(chf, left, 1, 0, threeMembers, capacityFactors(threeMembers));

      assertIsEquivalent(original, restarted);
   }

   private DefaultConsistentHash transition(ConsistentHashFactory<DefaultConsistentHash> chf, List<Address> threeMembers) {
      DefaultConsistentHash other = chf.create(2, 256, threeMembers.subList(0, 1), capacityFactors(threeMembers.subList(0, 1)));
      other = rebalanceIteration(chf, other, 1, 0, threeMembers.subList(0, 2), capacityFactors(threeMembers.subList(0, 2)));
      return rebalanceIteration(chf, other, 1, 0, threeMembers, capacityFactors(threeMembers));
   }

   private Map<Address, Float> capacityFactors(List<Address> members) {
      return members.stream()
            .collect(Collectors.toMap(Function.identity(), ignore -> 1.0f));
   }

   private void assertIsEquivalent(ConsistentHash oldConsistentHash, ConsistentHash newConsistentHash) {
      assertThat(oldConsistentHash.getNumSegments()).isEqualTo(newConsistentHash.getNumSegments());
      assertThat(oldConsistentHash.getMembers().size()).isEqualTo(newConsistentHash.getMembers().size());

      for (Address member : oldConsistentHash.getMembers()) {
         IntSet oldSegmentsForOwner = IntSets.from(oldConsistentHash.getSegmentsForOwner(member));
         IntSet newSegmentsForOwner = IntSets.from(newConsistentHash.getSegmentsForOwner(member));

         assertThat(oldSegmentsForOwner)
               .withFailMessage(() -> String.format("%s: Owners differ\n %s \n %s", member, oldSegmentsForOwner, newSegmentsForOwner))
               .isEqualTo(newSegmentsForOwner);

         IntSet oldPrimary = IntSets.from(oldConsistentHash.getPrimarySegmentsForOwner(member));
         IntSet newPrimary = IntSets.from(newConsistentHash.getPrimarySegmentsForOwner(member));
         assertThat(oldPrimary)
               .withFailMessage(() -> String.format("%s: Primary differ\n %s \n %s", member, oldPrimary, newPrimary))
               .isEqualTo(newPrimary);
      }

      assertThat(oldConsistentHash).isEqualTo(newConsistentHash);
   }
}
