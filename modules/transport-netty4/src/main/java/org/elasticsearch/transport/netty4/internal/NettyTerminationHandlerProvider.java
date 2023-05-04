/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4.internal;

import org.elasticsearch.node.internal.TerminationHandler;
import org.elasticsearch.node.internal.TerminationHandlerProvider;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.Collection;
import java.util.Collections;

public class NettyTerminationHandlerProvider implements TerminationHandlerProvider {
    private final Netty4Plugin plugin;

    public NettyTerminationHandlerProvider() {
        plugin = null;
    }

    public NettyTerminationHandlerProvider(Netty4Plugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<TerminationHandler> handlers() {
        return plugin != null ? plugin.getTerminationHandlers() : Collections.emptyList();
    }
}
