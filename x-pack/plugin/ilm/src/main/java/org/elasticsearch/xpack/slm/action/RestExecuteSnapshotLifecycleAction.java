/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;

public class RestExecuteSnapshotLifecycleAction extends BaseRestHandler {
    private static final Logger logger = LogManager.getLogger(RestExecuteSnapshotLifecycleAction.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public RestExecuteSnapshotLifecycleAction(RestController controller) {
        controller.registerWithDeprecatedHandler(RestRequest.Method.POST, "/_slm/policy/{name}/_execute", this,
            RestRequest.Method.PUT, "/_slm/policy/{name}/_execute", deprecationLogger);
    }

    @Override
    public String getName() {
        return "slm_execute_lifecycle";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String snapLifecycleId = request.param("name");
        ExecuteSnapshotLifecycleAction.Request req = new ExecuteSnapshotLifecycleAction.Request(snapLifecycleId);
        req.timeout(request.paramAsTime("timeout", req.timeout()));
        req.masterNodeTimeout(request.paramAsTime("master_timeout", req.masterNodeTimeout()));
        return channel -> client.execute(ExecuteSnapshotLifecycleAction.INSTANCE, req, new RestToXContentListener<>(channel));
    }
}
