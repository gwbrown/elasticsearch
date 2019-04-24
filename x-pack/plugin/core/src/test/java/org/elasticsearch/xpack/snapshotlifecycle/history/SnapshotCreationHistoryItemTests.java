/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle.history;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotInvocationRecordTests;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotInvocationRecordTests.randomSnapshotInvocationRecord;

public class SnapshotCreationHistoryItemTests extends AbstractSerializingTestCase<SnapshotCreationHistoryItem> {

    @Override
    protected SnapshotCreationHistoryItem doParseInstance(XContentParser parser) throws IOException {
        return SnapshotCreationHistoryItem.parse(parser, this.getClass().getCanonicalName());
    }

    @Override
    protected Writeable.Reader<SnapshotCreationHistoryItem> instanceReader() {
        return SnapshotCreationHistoryItem::new;
    }

    @Override
    protected SnapshotCreationHistoryItem createTestInstance() {
        long timestamp = randomNonNegativeLong();
        String policyId = randomAlphaOfLengthBetween(5, 10);
        String repository = randomAlphaOfLengthBetween(5, 10);
        String operation = randomAlphaOfLengthBetween(5, 10);
        boolean success = randomBoolean();
        SnapshotInvocationRecord result = randomSnapshotInvocationRecord();
        Map<String, Object> snapshotConfiguration = new HashMap<>();
        final int fields = randomIntBetween(2, 5);
        for (int i = 0; i < fields; i++) {
            snapshotConfiguration.put(randomAlphaOfLength(4), randomAlphaOfLength(4));
        }
        return new SnapshotCreationHistoryItem(timestamp, policyId, repository, operation, success, result, snapshotConfiguration);
    }

    @Override
    protected SnapshotCreationHistoryItem mutateInstance(SnapshotCreationHistoryItem instance) {
        final int branch = between(0, 5);
        switch (branch) {
            case 0:
                return new SnapshotCreationHistoryItem(
                    instance.getTimestamp(),
                    randomValueOtherThan(instance.getPolicyId(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getRepository(), instance.getOperation(), instance.isSuccess(), instance.getResult(),
                    instance.getSnapshotConfiguration());
            case 1:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(),
                    randomValueOtherThan(instance.getRepository(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.getOperation(), instance.isSuccess(), instance.getResult(), instance.getSnapshotConfiguration());
            case 2:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    randomValueOtherThan(instance.getOperation(), () -> randomAlphaOfLengthBetween(5, 10)),
                    instance.isSuccess(), instance.getResult(), instance.getSnapshotConfiguration());
            case 3:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getOperation(),
                    instance.isSuccess() == false,
                    instance.getResult(), instance.getSnapshotConfiguration());
            case 4:
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getOperation(),
                    instance.isSuccess(),
                    randomValueOtherThan(instance.getResult(), SnapshotInvocationRecordTests::randomSnapshotInvocationRecord),
                    instance.getSnapshotConfiguration());
            case 5:
                Map<String, Object> newConfig = new HashMap<>();
                final int fields = randomIntBetween(2, 5);
                for (int i = 0; i < fields; i++) {
                    newConfig.put(randomAlphaOfLength(3), randomAlphaOfLength(3));
                }
                return new SnapshotCreationHistoryItem(instance.getTimestamp(), instance.getPolicyId(), instance.getRepository(),
                    instance.getOperation(), instance.isSuccess(), instance.getResult(), newConfig);
            case 6:
                return new SnapshotCreationHistoryItem(
                    randomValueOtherThan(instance.getTimestamp(), ESTestCase::randomNonNegativeLong),
                    instance.getPolicyId(),
                    instance.getRepository(), instance.getOperation(), instance.isSuccess(), instance.getResult(),
                    instance.getSnapshotConfiguration());
            default:
                throw new IllegalArgumentException("illegal randomization: " + branch);
        }
    }
}
