package org.infinispan.client.hotrod.impl.operations;

/**
 * This is a marker interface only to tell dispatcher that this command should be retried.
 * Note that if additional capabilities are required this should be refactored to some sort of enum based
 * characteristics to allow more easily extensible features.
 */
public interface RetryingOperation {
}
