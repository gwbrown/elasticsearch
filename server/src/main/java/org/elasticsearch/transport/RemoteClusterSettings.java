/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.common.settings.Setting.listSetting;
import static org.elasticsearch.transport.TransportSettings.BIND_HOST_PROFILE;
import static org.elasticsearch.transport.TransportSettings.PORT_PROFILE;
import static org.elasticsearch.transport.TransportSettings.PUBLISH_HOST_PROFILE;
import static org.elasticsearch.transport.TransportSettings.PUBLISH_PORT_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_KEEP_ALIVE_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_KEEP_COUNT_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_KEEP_IDLE_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_KEEP_INTERVAL_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_NO_DELAY_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_RECEIVE_BUFFER_SIZE_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_REUSE_ADDRESS_PROFILE;
import static org.elasticsearch.transport.TransportSettings.TCP_SEND_BUFFER_SIZE_PROFILE;

public class RemoteClusterSettings {
    private static final Logger logger = LogManager.getLogger(RemoteClusterSettings.class);

    public static final String REMOTE_ACCESS_PROFILE = "_remote_access";
    public static final String REMOTE_ACCESS_PREFIX = "remote_cluster.";

    public static final Setting<Boolean> REMOTE_ACCESS_ENABLED = boolSetting(
        REMOTE_ACCESS_PREFIX + "enabled",
        false,
        Setting.Property.NodeScope
    );

    public static final Setting<List<String>> HOST = listSetting(
        REMOTE_ACCESS_PREFIX + "host",
        TransportSettings.BIND_HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> PUBLISH_HOST = listSetting(
        REMOTE_ACCESS_PREFIX + "publish_host",
        HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> BIND_HOST = listSetting(
        REMOTE_ACCESS_PREFIX + "bind_host",
        HOST,
        Function.identity(),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> PORT = intSetting(REMOTE_ACCESS_PREFIX + "port", 9400, 0, 65535, Setting.Property.NodeScope);

    public static final Setting<Integer> PUBLISH_PORT = intSetting(
        REMOTE_ACCESS_PREFIX + "publish_port",
        -1,
        -1,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> TCP_KEEP_ALIVE = boolSetting(
        REMOTE_ACCESS_PREFIX + "tcp.keep_alive",
        TransportSettings.TCP_KEEP_ALIVE,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> TCP_KEEP_IDLE = intSetting(
        REMOTE_ACCESS_PREFIX + "tcp.keep_idle",
        TransportSettings.TCP_KEEP_IDLE,
        -1,
        300,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> TCP_KEEP_INTERVAL = intSetting(
        REMOTE_ACCESS_PREFIX + "tcp.keep_interval",
        TransportSettings.TCP_KEEP_INTERVAL,
        -1,
        300,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> TCP_KEEP_COUNT = intSetting(
        REMOTE_ACCESS_PREFIX + "tcp.keep_interval",
        TransportSettings.TCP_KEEP_COUNT,
        -1,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> TCP_NO_DELAY = boolSetting(
        REMOTE_ACCESS_PREFIX + "tcp.no_delay",
        TransportSettings.TCP_NO_DELAY,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> TCP_REUSE_ADDRESS = boolSetting(
        REMOTE_ACCESS_PREFIX + "tcp.reuse_address",
        TransportSettings.TCP_REUSE_ADDRESS,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> TCP_SEND_BUFFER_SIZE = Setting.byteSizeSetting(
        REMOTE_ACCESS_PREFIX + "tcp.send_buffer_size",
        TransportSettings.TCP_SEND_BUFFER_SIZE,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> TCP_RECEIVE_BUFFER_SIZE = Setting.byteSizeSetting(
        REMOTE_ACCESS_PREFIX + "tcp.receive_buffer_size",
        TransportSettings.TCP_RECEIVE_BUFFER_SIZE,
        Setting.Property.NodeScope
    );

    static void validateRemoteAccessSettings(Settings settings) {
        if (settings.getGroups("transport.profiles.", true).keySet().contains(REMOTE_ACCESS_PROFILE)) {
            final List<String> conflicts = new ArrayList<>();
            if (settings.get(TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_KEEP_ALIVE.getKey()) != null) {
                conflicts.add(
                    TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()
                        + " and "
                        + TCP_KEEP_ALIVE.getKey()
                );
            }
            if (settings.get(TCP_KEEP_IDLE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_KEEP_IDLE.getKey()) != null) {
                conflicts.add(
                    TCP_KEEP_IDLE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey() + " and " + TCP_KEEP_IDLE.getKey()
                );

            }
            if (settings.get(TCP_KEEP_INTERVAL_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_KEEP_INTERVAL.getKey()) != null) {
                conflicts.add(
                    TCP_KEEP_INTERVAL_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()
                        + " and "
                        + TCP_KEEP_INTERVAL.getKey()
                );

            }
            if (settings.get(TCP_KEEP_COUNT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_KEEP_COUNT.getKey()) != null) {
                conflicts.add(
                    TCP_KEEP_COUNT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()
                        + " and "
                        + TCP_KEEP_COUNT.getKey()
                );

            }
            if (settings.get(TCP_NO_DELAY_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_NO_DELAY.getKey()) != null) {
                conflicts.add(
                    TCP_NO_DELAY_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey() + " and " + TCP_NO_DELAY.getKey()
                );

            }
            if (settings.get(TCP_REUSE_ADDRESS_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_REUSE_ADDRESS.getKey()) != null) {
                conflicts.add(
                    TCP_REUSE_ADDRESS_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()
                        + " and "
                        + TCP_NO_DELAY.getKey()
                );

            }
            if (settings.get(TCP_SEND_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_SEND_BUFFER_SIZE.getKey()) != null) {
                conflicts.add(
                    TCP_SEND_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()
                        + " and "
                        + TCP_SEND_BUFFER_SIZE.getKey()
                );

            }
            if (settings.get(TCP_RECEIVE_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(TCP_RECEIVE_BUFFER_SIZE.getKey()) != null) {
                conflicts.add(
                    TCP_RECEIVE_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()
                        + " and "
                        + TCP_RECEIVE_BUFFER_SIZE.getKey()
                );

            }
            if (settings.get(BIND_HOST_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(BIND_HOST.getKey()) != null) {
                conflicts.add(
                    BIND_HOST_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey() + " and " + BIND_HOST.getKey()
                );

            }
            if (settings.get(PUBLISH_HOST_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(PUBLISH_HOST.getKey()) != null) {
                conflicts.add(
                    PUBLISH_HOST_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey() + " and " + PUBLISH_HOST.getKey()
                );

            }
            if (settings.get(PORT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(PORT.getKey()) != null) {
                conflicts.add(PORT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey() + " and " + PORT.getKey());
            }
            if (settings.get(PUBLISH_PORT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey()) != null
                && settings.get(PUBLISH_PORT.getKey()) != null) {
                conflicts.add(
                    PUBLISH_PORT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey() + " and " + PUBLISH_PORT.getKey()
                );
            }

            if (conflicts.isEmpty() == false) {
                final String message = "There are conflicts between remote access settings and transport profile "
                    + "[_remote_access]. These pairs of settings conflict, remove one of each pair: "
                    + conflicts;
                if (REMOTE_ACCESS_ENABLED.get(settings)) {
                    throw new IllegalStateException(message);
                } else {
                    logger.warn("{} or Elasticsearch will fail to start once [{}] is set to true", message, REMOTE_ACCESS_ENABLED.getKey());
                }

            }
        }
    }

    public static TcpTransport.ProfileSettings buildRemoteAccessProfileSettings(Settings settings) {
        validateRemoteAccessSettings(settings);

        // Build a synthetic settings object with the `_remote_access` profile properly configured per the friendlier settings,
        // but inherit any `_remote_access` profile settings that don't conflict.
        Settings syntheticRemoteAccessProfile = Settings.builder()
            .put(settings)
            .put(
                TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(),
                TCP_KEEP_ALIVE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).get(settings)
            )
            .put(TCP_KEEP_IDLE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), TCP_KEEP_IDLE.get(settings))
            .put(TCP_KEEP_INTERVAL_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), TCP_KEEP_INTERVAL.get(settings))
            .put(TCP_KEEP_COUNT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), TCP_KEEP_COUNT.get(settings))
            .put(TCP_NO_DELAY_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), TCP_NO_DELAY.get(settings))
            .put(TCP_REUSE_ADDRESS_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), TCP_REUSE_ADDRESS.get(settings))
            .put(
                TCP_SEND_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(),
                TCP_SEND_BUFFER_SIZE.get(settings)
            )
            .put(
                TCP_RECEIVE_BUFFER_SIZE_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(),
                TCP_RECEIVE_BUFFER_SIZE.get(settings)
            )
            .putList(BIND_HOST_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), BIND_HOST.get(settings))
            .putList(PUBLISH_HOST_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), PUBLISH_HOST.get(settings))
            .put(PORT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), PORT.get(settings))
            .put(PUBLISH_PORT_PROFILE.getConcreteSettingForNamespace(REMOTE_ACCESS_PROFILE).getKey(), PUBLISH_PORT.get(settings))
            .build();
        return new TcpTransport.ProfileSettings(syntheticRemoteAccessProfile, REMOTE_ACCESS_PROFILE);
    }
}
