/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle.history;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotInvocationRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SnapshotCreationHistoryItem extends SnapshotHistoryItem {

    private final Map<String, Object> snapshotConfiguration;
    private final SnapshotInvocationRecord result;

    static final ParseField SNAPSHOT_CONFIG = new ParseField("configuration");
    static final ParseField RESULT = new ParseField("creation_result");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SnapshotCreationHistoryItem, String> PARSER =
        new ConstructingObjectParser<>("snapshot_lifecycle_history_item", true,
            (a, id) -> {
                final long timestamp = (long) a[0];
                final String policyId = (String) a[1];
                final String repository = (String) a[2];
                final String operation = (String) a[3];
                final boolean success = (boolean) a[4];
                final Map<String, Object> snapshotConfiguration = (Map<String, Object>) a[5];
                final SnapshotInvocationRecord result = (SnapshotInvocationRecord) a[6];
                return new SnapshotCreationHistoryItem(timestamp, policyId, repository, operation, success, result, snapshotConfiguration);
            });

    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), POLICY_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), OPERATION);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SUCCESS);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), SNAPSHOT_CONFIG);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotInvocationRecord::parse, RESULT);
    }

    public SnapshotCreationHistoryItem(long timestamp, String policyId, String repository, String operation, boolean success,
                                       SnapshotInvocationRecord result, Map<String, Object> snapshotConfiguration) {
        super(timestamp, policyId, repository, operation, success);
        this.snapshotConfiguration = Objects.requireNonNull(snapshotConfiguration);
        this.result = Objects.requireNonNull(result);
    }

    public SnapshotCreationHistoryItem(StreamInput in) throws IOException {
        super(in);
        this.snapshotConfiguration = in.readMap();
        this.result = new SnapshotInvocationRecord(in);
    }

    public Map<String, Object> getSnapshotConfiguration() {
        return snapshotConfiguration;
    }

    public SnapshotInvocationRecord getResult() {
        return result;
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeMap(snapshotConfiguration);
        result.writeTo(out);
    }

    @Override
    protected XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SNAPSHOT_CONFIG.getPreferredName(), snapshotConfiguration);
        builder.field(RESULT.getPreferredName(), result);
        return builder;
    }

    public static SnapshotCreationHistoryItem parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        SnapshotCreationHistoryItem that = (SnapshotCreationHistoryItem) o;
        return Objects.equals(getSnapshotConfiguration(), that.getSnapshotConfiguration()) &&
            Objects.equals(getResult(), that.getResult());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getSnapshotConfiguration(), getResult());
    }
}
