/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Traceable;
import org.elasticsearch.tasks.Tracer;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface ClusterStateTaskExecutor<T> {
    /**
     * Update the cluster state based on the current state and the given tasks. Return the *same instance* if no state
     * should be changed.
     */
    ClusterTasksResult<T> execute(ClusterState currentState, List<TraceableTask<T>> tasks, Tracer tracer) throws Exception;

    /**
     * indicates whether this executor should only run if the current node is master
     */
    default boolean runOnlyOnMaster() {
        return true;
    }

    /**
     * Callback invoked after new cluster state is published. Note that
     * this method is not invoked if the cluster state was not updated.
     *
     * Note that this method will be executed using system context.
     *
     * @param clusterStatePublicationEvent the change event for this cluster state publication, containing both old and new states
     */
    default void clusterStatePublished(ClusterStatePublicationEvent clusterStatePublicationEvent) {}

    /**
     * Builds a concise description of a list of tasks (to be used in logging etc.).
     *
     * Note that the tasks given are not necessarily the same as those that will be passed to {@link #execute(ClusterState, List, Tracer)}.
     * but are guaranteed to be a subset of them. This method can be called multiple times with different lists before execution.
     * This allows groupd task description but the submitting source.
     */
    default String describeTasks(List<T> tasks) {
        final StringBuilder output = new StringBuilder();
        Strings.collectionToDelimitedStringWithLimit(
            (Iterable<String>) () -> tasks.stream().map(Object::toString).filter(s -> s.isEmpty() == false).iterator(),
            ", ",
            "",
            "",
            1024,
            output
        );
        return output.toString();
    }

    /**
     * Represents the result of a batched execution of cluster state update tasks
     * @param <T> the type of the cluster state update task
     */
    class ClusterTasksResult<T> {
        @Nullable
        public final ClusterState resultingState;
        public final Map<T, TaskResult> executionResults;

        /**
         * Construct an execution result instance with a correspondence between the tasks and their execution result
         * @param resultingState the resulting cluster state
         * @param executionResults the correspondence between tasks and their outcome
         */
        ClusterTasksResult(ClusterState resultingState, Map<T, TaskResult> executionResults) {
            this.resultingState = resultingState;
            this.executionResults = executionResults;
        }

        public static <T> Builder<T> builder() {
            return new Builder<>();
        }

        public static class Builder<T> {
            private final Map<T, TaskResult> executionResults = new IdentityHashMap<>();

            public Builder<T> success(T task) {
                return result(task, TaskResult.success());
            }

            public Builder<T> successes(Iterable<T> tasks) {
                for (T task : tasks) {
                    success(task);
                }
                return this;
            }

            public Builder<T> failure(T task, Exception e) {
                return result(task, TaskResult.failure(e));
            }

            public Builder<T> failures(Iterable<T> tasks, Exception e) {
                for (T task : tasks) {
                    failure(task, e);
                }
                return this;
            }

            private Builder<T> result(T task, TaskResult executionResult) {
                TaskResult existing = executionResults.put(task, executionResult);
                assert existing == null : task + " already has result " + existing;
                return this;
            }

            public ClusterTasksResult<T> build(ClusterState resultingState) {
                return new ClusterTasksResult<>(resultingState, executionResults);
            }

            ClusterTasksResult<T> build(ClusterTasksResult<T> result, ClusterState previousState) {
                return new ClusterTasksResult<>(result.resultingState == null ? previousState : result.resultingState, executionResults);
            }
        }
    }

    final class TaskResult {
        private final Exception failure;

        private static final TaskResult SUCCESS = new TaskResult(null);

        public static TaskResult success() {
            return SUCCESS;
        }

        public static TaskResult failure(Exception failure) {
            return new TaskResult(failure);
        }

        private TaskResult(Exception failure) {
            this.failure = failure;
        }

        public boolean isSuccess() {
            return this == SUCCESS;
        }

        public Exception getFailure() {
            assert isSuccess() == false;
            return failure;
        }
    }

    class TraceableTask<T> implements Traceable {
        private final T item;
        private final Traceable traceable;

        public TraceableTask(T item, Traceable traceable) {
            this.item = item;
            this.traceable = traceable;
        }

        public void processTask(Tracer tracer, Consumer<T> taskProcessor) {
            try {
                tracer.onTraceStopped(traceable);
                taskProcessor.accept(item);
            } finally {
                tracer.onTraceStopped(traceable);
            }
        }

        public T task() {
            return item;
        }

        @Override
        public String getSpanId() {
            return traceable.getSpanId();
        }

        @Override
        public String getSpanName() {
            return traceable.getSpanName();
        }

        @Override
        public Map<String, Object> getAttributes() {
            return traceable.getAttributes();
        }

        @Override
        public String getTraceParent() {
            return traceable.getTraceParent();
        }

        @Override
        public String getTraceState() {
            return traceable.getTraceState();
        }
    }
}
