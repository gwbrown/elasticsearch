/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public interface Tracer {
    void onTraceStarted(Traceable traceable);

    void onTraceStopped(Traceable traceable);

    class CompoundTracer implements Tracer {
        private static final Logger logger = LogManager.getLogger();

        private final List<Tracer> tracers;

        public CompoundTracer(List<Tracer> tracers) {
            this.tracers = tracers;
        }

        @Override
        public void onTraceStarted(Traceable traceable) {
            for (Tracer tracer : tracers) {
                try {
                    tracer.onTraceStarted(traceable);
                } catch (Exception e) {
                    logger.error("tracer [{}] failed while tracing [{}]", tracer, traceable);
                }
            }
        }

        @Override
        public void onTraceStopped(Traceable traceable) {
            for (Tracer tracer : tracers) {
                try {
                    tracer.onTraceStopped(traceable);
                } catch (Exception e) {
                    logger.error("tracer [{}] failed while tracing [{}]", tracer, traceable);
                }
            }
        }
    }

    /**
     * A no-op Tracer, to provide sensible defaults for testing.
     */
    class NoopTracer implements Tracer {

        @Override
        public void onTraceStarted(Traceable traceable) {
            // This space intentionally left blank
        }

        @Override
        public void onTraceStopped(Traceable traceable) {
            // This space intentionally left blank
        }
    }
}
