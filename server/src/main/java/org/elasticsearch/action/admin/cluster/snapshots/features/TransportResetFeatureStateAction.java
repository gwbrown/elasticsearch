/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.features;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportResetFeatureStateAction extends TransportMasterNodeAction<ResetFeatureStateRequest, ResetFeatureStateResponse> {

    @Inject
    protected TransportResetFeatureStateAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<ResetFeatureStateRequest> request,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Writeable.Reader<ResetFeatureStateResponse> response,
        String executor
    ) {
        super(actionName, transportService, clusterService, threadPool, actionFilters,
            request, indexNameExpressionResolver, response, executor);
    }

    @Override
    protected void masterOperation(
        Task task,
        ResetFeatureStateRequest request,
        ClusterState state,
        ActionListener<ResetFeatureStateResponse> listener
    ) throws Exception {
        // TODO[wrb]
    }

    @Override
    protected ClusterBlockException checkBlock(ResetFeatureStateRequest request, ClusterState state) {
        // TODO[wrb]
        return null;
    }
}
