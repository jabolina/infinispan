package org.infinispan.persistence.manager;

public enum PreloadStatus {

   NOT_RUNNING,

   RUNNING,

   COMPLETE_LOAD,

   PARTIAL_LOAD,

   FAILED_LOAD;

   public boolean fullyPreloaded() {
      return this == COMPLETE_LOAD;
   }
}
