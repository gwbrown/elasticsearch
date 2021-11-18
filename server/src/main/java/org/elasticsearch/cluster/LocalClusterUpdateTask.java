/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Tracer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to apply state updates on nodes that are not necessarily master
 */
public abstract class LocalClusterUpdateTask
    implements
        ClusterStateTaskConfig,
        ClusterStateTaskExecutor<LocalClusterUpdateTask>,
        ClusterStateTaskListener {

    private final Priority priority;

    public LocalClusterUpdateTask() {
        this(Priority.NORMAL);
    }

    public LocalClusterUpdateTask(Priority priority) {
        this.priority = priority;
    }

    public abstract ClusterTasksResult<LocalClusterUpdateTask> execute(ClusterState currentState) throws Exception;

    @Override
    public final ClusterTasksResult<LocalClusterUpdateTask> execute(
        ClusterState currentState,
        List<TraceableTask<LocalClusterUpdateTask>> tasks,
        Tracer tracer
    ) throws Exception {
        assert tasks.size() == 1 && tasks.get(0).task() == this
            : "expected one-element task list containing current object but was " + tasks;
        try {
            tracer.onTraceStarted(tasks.get(0));
            ClusterTasksResult<LocalClusterUpdateTask> result = execute(currentState);
            return ClusterTasksResult.<LocalClusterUpdateTask>builder()
                .successes(tasks.stream().map(TraceableTask::task).collect(Collectors.toList()))
                .build(result, currentState);
        } finally {
            tracer.onTraceStopped(tasks.get(0));
        }
    }

    /**
     * no changes were made to the cluster state. Useful to execute a runnable on the cluster state applier thread
     */
    public static ClusterTasksResult<LocalClusterUpdateTask> unchanged() {
        return new ClusterTasksResult<>(null, null);
    }

    @Override
    public String describeTasks(List<LocalClusterUpdateTask> tasks) {
        return ""; // one of task, source is enough
    }

    @Nullable
    public TimeValue timeout() {
        return null;
    }

    @Override
    public Priority priority() {
        return priority;
    }

    @Override
    public final boolean runOnlyOnMaster() {
        return false;
    }
}
