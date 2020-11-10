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
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.stream.Collectors;

public class TransportSnapshottableFeaturesAction extends TransportMasterNodeAction<GetSnapshottableFeaturesRequest,
    GetSnapshottableFeaturesResponse> {

    private final SystemIndices systemIndices;

    @Inject
    public TransportSnapshottableFeaturesAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                SystemIndices systemIndices) {
        super(SnapshottableFeaturesAction.NAME, transportService, clusterService, threadPool, actionFilters,
            GetSnapshottableFeaturesRequest::new, indexNameExpressionResolver, GetSnapshottableFeaturesResponse::new,
            ThreadPool.Names.SAME);
        this.systemIndices = systemIndices;
    }

    @Override
    protected void masterOperation(Task task, GetSnapshottableFeaturesRequest request, ClusterState state,
                                   ActionListener<GetSnapshottableFeaturesResponse> listener) throws Exception {
        listener.onResponse(new GetSnapshottableFeaturesResponse(systemIndices.getFeatures().stream()
            .map(GetSnapshottableFeaturesResponse.SnapshottableFeature::new)
            .collect(Collectors.toList())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetSnapshottableFeaturesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
