/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Tracer;

import java.util.List;
import java.util.stream.Collectors;

public class NodeRemovalClusterStateTaskExecutor implements ClusterStateTaskExecutor<NodeRemovalClusterStateTaskExecutor.Task> {

    private final AllocationService allocationService;
    private final Logger logger;

    public static class Task {

        private final DiscoveryNode node;
        private final String reason;

        public Task(final DiscoveryNode node, final String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            final StringBuilder stringBuilder = new StringBuilder();
            node.appendDescriptionWithoutAttributes(stringBuilder);
            stringBuilder.append(" reason: ").append(reason);
            return stringBuilder.toString();
        }
    }

    public NodeRemovalClusterStateTaskExecutor(final AllocationService allocationService, final Logger logger) {
        this.allocationService = allocationService;
        this.logger = logger;
    }

    @Override
    public ClusterTasksResult<Task> execute(final ClusterState currentState, final List<TraceableTask<Task>> tasks, Tracer tracer)
        throws Exception {
        final DiscoveryNodes.Builder remainingNodesBuilder = DiscoveryNodes.builder(currentState.nodes());
        boolean removed = false;
        for (final TraceableTask<Task> traceableTask : tasks) {
            final Task task = traceableTask.task(); // There's not really a meaningful way to trace this, so don't
            if (currentState.nodes().nodeExists(task.node())) {
                remainingNodesBuilder.remove(task.node());
                removed = true;
            } else {
                logger.debug("node [{}] does not exist in cluster state, ignoring", task);
            }
        }
        List<Task> rawTasks = tasks.stream().map(TraceableTask::task).collect(Collectors.toList());

        if (removed == false) {
            // no nodes to remove, keep the current cluster state
            return ClusterTasksResult.<Task>builder().successes(rawTasks).build(currentState);
        }

        final ClusterState remainingNodesClusterState = remainingNodesClusterState(currentState, remainingNodesBuilder);
        final ClusterState ptasksDisassociatedState = PersistentTasksCustomMetadata.disassociateDeadNodes(remainingNodesClusterState);
        final ClusterState finalState = allocationService.disassociateDeadNodes(ptasksDisassociatedState, true, describeTasks(rawTasks));

        return ClusterTasksResult.<Task>builder().successes(rawTasks).build(finalState);
    }

    // visible for testing
    // hook is used in testing to ensure that correct cluster state is used to test whether a
    // rejoin or reroute is needed
    protected ClusterState remainingNodesClusterState(final ClusterState currentState, DiscoveryNodes.Builder remainingNodesBuilder) {
        return ClusterState.builder(currentState).nodes(remainingNodesBuilder).build();
    }

}
