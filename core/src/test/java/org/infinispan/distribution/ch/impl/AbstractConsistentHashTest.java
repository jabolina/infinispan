package org.infinispan.distribution.ch.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.TestAddress;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;

abstract class AbstractConsistentHashTest extends AbstractInfinispanTest {

   private int iterationCount = 0;

   protected abstract int[][] nodeChanges();

   protected final void testConsistentHashModifications(ConsistentHashFactory<DefaultConsistentHash> chf,
                                                List<Address> nodes, int ns, int no, Map<Address, Float> capacityFactors) {
      log.tracef("Creating consistent hash with ns=%d, no=%d, members=(%d)%s",
            ns, no, nodes.size(), membersString(nodes, capacityFactors));
      DefaultConsistentHash baseCH = chf.create(no, ns, nodes, capacityFactors);
      assertEquals(baseCH.getCapacityFactors(), capacityFactors);
      checkDistribution(baseCH, capacityFactors);

      // check that the base CH is already balanced
      List<Address> baseMembers = baseCH.getMembers();
      assertSame(baseCH, chf.updateMembers(baseCH, baseMembers, capacityFactors));
      assertSame(baseCH, chf.rebalance(baseCH));

      // starting point, so that we don't confuse nodes
      int nodeIndex = baseMembers.size();

      for (int[] nodeChange : nodeChanges()) {
         int nodesToAdd = nodeChange[0];
         int nodesToRemove = nodeChange[1];
         if (nodesToRemove > baseMembers.size())
            break;
         if (nodesToRemove == baseMembers.size() && nodesToAdd == 0)
            break;

         List<Address> newMembers = new ArrayList<>(baseMembers);
         HashMap<Address, Float> newCapacityFactors = capacityFactors != null ? new HashMap<>(capacityFactors) : null;
         for (int k = 0; k < nodesToRemove; k++) {
            int indexToRemove = Math.abs(MurmurHash3.getInstance().hash(k) % newMembers.size());
            if (newCapacityFactors != null) {
               newCapacityFactors.remove(newMembers.get(indexToRemove));
            }
            newMembers.remove(indexToRemove);
         }
         for (int k = 0; k < nodesToAdd; k++) {
            TestAddress address = new TestAddress(nodeIndex++, "TA");
            newMembers.add(address);
            if (newCapacityFactors != null) {
               newCapacityFactors.put(address, capacityFactors.get(baseMembers.get(k % baseMembers.size())));
            }
         }

         log.tracef("Rebalance iteration %d, members=(%d)%s",
               iterationCount, newMembers.size(), membersString(newMembers, newCapacityFactors));
         baseCH = rebalanceIteration(chf, baseCH, nodesToAdd, nodesToRemove, newMembers, newCapacityFactors);
         baseMembers = baseCH.getMembers();
         capacityFactors = newCapacityFactors;

         iterationCount++;
      }
   }

   protected final DefaultConsistentHash rebalanceIteration(ConsistentHashFactory<DefaultConsistentHash> chf,
                                                    DefaultConsistentHash baseCH, int nodesToAdd,
                                                    int nodesToRemove, List<Address> newMembers,
                                                    Map<Address, Float> lfMap) {
      int actualNumOwners = computeActualNumOwners(baseCH.getNumOwners(), newMembers, lfMap);

      // first phase: just update the members list, removing the leavers
      // and adding new owners, but not necessarily assigning segments to them
      DefaultConsistentHash updatedMembersCH = chf.updateMembers(baseCH, newMembers, lfMap);
      assertEquals(lfMap, updatedMembersCH.getCapacityFactors());
      if (nodesToRemove > 0) {
         for (int l = 0; l < updatedMembersCH.getNumSegments(); l++) {
            assertTrue(!updatedMembersCH.locateOwnersForSegment(l).isEmpty());
            assertTrue(updatedMembersCH.locateOwnersForSegment(l).size() <= actualNumOwners);
         }
      }

      // second phase: rebalance with the new members list
      long startNanos = System.nanoTime();
      DefaultConsistentHash rebalancedCH = chf.rebalance(updatedMembersCH);
      long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
      if (durationMillis >= 5) {
         log.tracef("Rebalance took %dms", durationMillis);
      }
      checkDistribution(rebalancedCH, lfMap);

      for (int l = 0; l < rebalancedCH.getNumSegments(); l++) {
         assertTrue(rebalancedCH.locateOwnersForSegment(l).size() >= actualNumOwners);
      }

      checkMovedSegments(baseCH, rebalancedCH, nodesToAdd, nodesToRemove);

      // union doesn't have to keep the CH balanced, but it does have to include owners from both CHs
      DefaultConsistentHash unionCH = chf.union(updatedMembersCH, rebalancedCH);
      for (int l = 0; l < updatedMembersCH.getNumSegments(); l++) {
         assertTrue(unionCH.locateOwnersForSegment(l).containsAll(updatedMembersCH.locateOwnersForSegment(l)));
         assertTrue(unionCH.locateOwnersForSegment(l).containsAll(rebalancedCH.locateOwnersForSegment(l)));
      }

      // switch to the new CH in the next iteration
      assertEquals(baseCH.getNumSegments(), rebalancedCH.getNumSegments());
      assertEquals(baseCH.getNumOwners(), rebalancedCH.getNumOwners());
      assertEquals(newMembers, rebalancedCH.getMembers());
      baseCH = rebalancedCH;
      return baseCH;
   }

   private String membersString(List<Address> newMembers, Map<Address, Float> newCapacityFactors) {
      return newMembers.stream()
            .map(a -> String.format("%s * %.1f", a, getCapacityFactor(newCapacityFactors, a)))
            .collect(Collectors.joining(", ", "[", "]"));
   }

   protected final int computeActualNumOwners(int numOwners, List<Address> members, Map<Address, Float> capacityFactors) {
      int nodesWithLoad = nodesWithLoad(members, capacityFactors);
      return Math.min(numOwners, nodesWithLoad);
   }

   protected final int nodesWithLoad(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return members.size();

      int nodesWithLoad = 0;
      for (Address node : members) {
         if (capacityFactors.get(node) != 0) {
            nodesWithLoad++;
         }
      }
      return nodesWithLoad;
   }

   private void checkMovedSegments(DefaultConsistentHash oldCH, DefaultConsistentHash newCH,
                                   int nodesAdded, int nodesRemoved) {
      int numSegments = oldCH.getNumSegments();
      int numOwners = oldCH.getNumOwners();
      List<Address> oldMembers = oldCH.getMembers();
      List<Address> newMembers = newCH.getMembers();
      Set<Address> commonMembers = new HashSet<>(oldMembers);
      commonMembers.retainAll(newMembers);

      // Compute the number of segments owned by members that left or joined
      int leaverSegments = 0;
      for (Address node : oldMembers) {
         if (!commonMembers.contains(node)) {
            leaverSegments += oldCH.getSegmentsForOwner(node).size();
         }
      }
      int joinerSegments = 0;
      for (Address node : newMembers) {
         if (!commonMembers.contains(node)) {
            joinerSegments += newCH.getSegmentsForOwner(node).size();
         }
      }

      // Compute the number of segments where a common member added/removed segments
      int commonMembersAddedSegments = 0;
      int commonMembersRemovedSegments = 0;
      int primarySwitchedWithBackup = 0;
      for (int segment = 0; segment < numSegments; segment++) {
         List<Address> oldOwners = oldCH.locateOwnersForSegment(segment);
         List<Address> newOwners = newCH.locateOwnersForSegment(segment);
         for (Address newOwner : newOwners) {
            if (commonMembers.contains(newOwner) && !oldOwners.contains(newOwner)) {
               commonMembersAddedSegments++;
            }
         }

         for (Address oldOwner : oldOwners) {
            if (commonMembers.contains(oldOwner) && !newOwners.contains(oldOwner)) {
               commonMembersRemovedSegments++;
            }
         }

         Address oldPrimary = oldOwners.get(0);
         Address newPrimary = newOwners.get(0);
         if (!newPrimary.equals(oldPrimary) && newOwners.contains(oldPrimary) && oldOwners.contains(newPrimary)) {
            primarySwitchedWithBackup++;
         }
      }

      // When we have both joiners and leavers, leaverSegments may be > commonMembersAddedSegments
      int movedSegments = Math.max(0, commonMembersAddedSegments - leaverSegments);
      // When nodes with load < numOwners, commonMembersLostSegments is 0 but joinerSegments > 0
      int movedSegments2 = Math.max(0, commonMembersRemovedSegments - joinerSegments);
      assertEquals(movedSegments, movedSegments2);
      int expectedExtraMoves = (int) Math.ceil(allowedExtraMoves(oldCH, newCH, joinerSegments, leaverSegments));
      if (movedSegments > expectedExtraMoves / 2) {
         log.tracef("%d of %d*%d extra segments moved, %fx of allowed (%d), %d leavers had %d, %d joiners have %d",
               movedSegments, numOwners, numSegments, (float) movedSegments / expectedExtraMoves,
               expectedExtraMoves, nodesRemoved, leaverSegments, nodesAdded, joinerSegments);
      }
      if (movedSegments > expectedExtraMoves) {
         fail(String.format("Two many moved segments between %s and %s: expected %d, got %d",
               oldCH, newCH, expectedExtraMoves, movedSegments));
      }

      if (primarySwitchedWithBackup > Math.ceil(0.05 * numSegments)) {
         log.tracef("Primary owner switched with backup for %d segments of %d", primarySwitchedWithBackup, numSegments);
      }
      double acceptablePrimarySwitchedWithBackup = Math.ceil(0.5 * (joinerSegments + leaverSegments) / numOwners * numSegments);
      if (primarySwitchedWithBackup > acceptablePrimarySwitchedWithBackup) {
         fail(String.format("Primary owner switched with backup owner for too many segments: %d of %d", primarySwitchedWithBackup, numSegments));
      }
   }

   protected float allowedExtraMoves(DefaultConsistentHash oldCH, DefaultConsistentHash newCH,
                                     int joinerSegments, int leaverSegments) {
      return Math.max(1, 0.1f * oldCH.getNumOwners() * oldCH.getNumSegments());
   }

   protected final void checkDistribution(DefaultConsistentHash ch, Map<Address, Float> lfMap) {
      int numSegments = ch.getNumSegments();
      List<Address> nodes = ch.getMembers();
      int numNodesWithLoad = nodesWithLoad(nodes, lfMap);
      int actualNumOwners = computeActualNumOwners(ch.getNumOwners(), nodes, lfMap);

      OwnershipStatistics stats = new OwnershipStatistics(ch, nodes);
      for (int s = 0; s < numSegments; s++) {
         List<Address> owners = ch.locateOwnersForSegment(s);
         assertEquals(actualNumOwners, owners.size());
         for (int i = 1; i < owners.size(); i++) {
            Address owner = owners.get(i);
            assertEquals("Found the same owner twice in the owners list", i, owners.indexOf(owner));
         }
      }

      float totalCapacity = computeTotalCapacity(nodes, lfMap);
      Map<Address, Float> expectedOwnedMap =
            computeExpectedOwned(numSegments, numNodesWithLoad, actualNumOwners, nodes, lfMap);
      for (Address node : nodes) {
         float capacityFactor = getCapacityFactor(lfMap, node);
         float expectedPrimaryOwned = expectedPrimaryOwned(numSegments, numNodesWithLoad, totalCapacity, capacityFactor);
         int minPrimaryOwned = (int) Math.floor(minOwned(numSegments, 1, numNodesWithLoad, expectedPrimaryOwned));
         int maxPrimaryOwned = (int) Math.ceil(maxOwned(numSegments, 1, numNodesWithLoad, expectedPrimaryOwned));
         int primaryOwned = stats.getPrimaryOwned(node);
         if (primaryOwned < minPrimaryOwned || maxPrimaryOwned < primaryOwned) {
            fail(String.format("Primary owned (%d) should have been between %d and %d", primaryOwned, minPrimaryOwned, maxPrimaryOwned));
         }

         float expectedOwned = expectedOwnedMap.get(node);
         int minOwned = (int) Math.floor(minOwned(numSegments, actualNumOwners, numNodesWithLoad, expectedOwned));
         int maxOwned = (int) Math.ceil(maxOwned(numSegments, actualNumOwners, numNodesWithLoad, expectedOwned));
         int owned = stats.getOwned(node);
         if (owned < minOwned || maxOwned < owned) {
            fail(String.format("Owned (%d) should have been between %d and %d", owned, minOwned, maxOwned));
         }
      }
   }

   protected float expectedPrimaryOwned(int numSegments, int numNodes, float totalCapacity, float nodeLoad) {
      return numSegments * nodeLoad / totalCapacity;
   }

   private Map<Address, Float> computeExpectedOwned(int numSegments, int numNodes, int actualNumOwners,
                                                      Collection<Address> nodes, Map<Address, Float> capacityFactors) {
      // Insert all nodes in the initial order, even if we're going to replace the values later
      Map<Address, Float> expectedOwned = new LinkedHashMap<>(numNodes * 2);
      float expected = Math.min(numSegments, (float) numSegments * actualNumOwners / numNodes);
      for (Address node : nodes) {
         expectedOwned.put(node, expected);
      }
      if (capacityFactors == null)
         return expectedOwned;

      List<Address> sortedNodes = new ArrayList<>(nodes);
      sortedNodes.sort((o1, o2) -> {
         // Reverse order
         return Float.compare(capacityFactors.get(o2), capacityFactors.get(o1));
      });

      float totalCapacity = computeTotalCapacity(nodes, capacityFactors);

      int remainingCopies = actualNumOwners * numSegments;
      for (Address node : sortedNodes) {
         float nodeLoad = capacityFactors.get(node);
         float nodeSegments;
         if (remainingCopies * nodeLoad / totalCapacity > numSegments) {
            nodeSegments = numSegments;
            totalCapacity -= nodeLoad;
            remainingCopies -= nodeSegments;
         } else {
            nodeSegments = nodeLoad != 0 ? remainingCopies * nodeLoad / totalCapacity : 0;
         }
         expectedOwned.put(node, nodeSegments);
      }
      return expectedOwned;
   }

   protected float maxOwned(int numSegments, int actualNumOwners, int numNodes, float expectedOwned) {
      return expectedOwned + (numNodes - 1) + .01f * expectedOwned;
   }

   protected float minOwned(int numSegments, int actualNumOwners, int numNodes, float expectedOwned) {
      return expectedOwned - Math.max(1, (numSegments * actualNumOwners) / expectedOwned * numNodes);
   }

   private float computeTotalCapacity(Collection<Address> nodes, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null)
         return nodes.size();

      float totalCapacity = 0;
      for (Address node : nodes) {
         totalCapacity += capacityFactors.get(node);
      }
      return totalCapacity;
   }

   private float getCapacityFactor(Map<Address, Float> capacityFactors, Address a) {
      return capacityFactors != null ? capacityFactors.get(a) : 1f;
   }
}
