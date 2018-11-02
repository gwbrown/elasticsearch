/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.Objects;

public class IndexLifecycleExplainResponse implements ToXContentObject, Writeable {

    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField MANAGED_BY_ILM_FIELD = new ParseField("managed");
    private static final ParseField POLICY_NAME_FIELD = new ParseField("policy");
    private static final ParseField LIFECYCLE_DATE_FIELD = new ParseField("lifecycle_date");
    private static final ParseField PHASE_FIELD = new ParseField("phase");
    private static final ParseField ACTION_FIELD = new ParseField("action");
    private static final ParseField STEP_FIELD = new ParseField("step");
    private static final ParseField FAILED_STEP_FIELD = new ParseField("failed_step");
    private static final ParseField PHASE_TIME_FIELD = new ParseField("phase_time");
    private static final ParseField ACTION_TIME_FIELD = new ParseField("action_time");
    private static final ParseField STEP_TIME_FIELD = new ParseField("step_time");
    private static final ParseField STEP_INFO_FIELD = new ParseField("step_info");
    private static final ParseField PHASE_EXECUTION_INFO = new ParseField("phase_execution");

    public static final ConstructingObjectParser<IndexLifecycleExplainResponse, Void> PARSER = new ConstructingObjectParser<>(
            "index_lifecycle_explain_response",
            a -> new IndexLifecycleExplainResponse(
                    (String) a[0],
                    (boolean) a[1],
                    (String) a[2],
                    (Long) (a[3]),
                    (String) a[4],
                    (String) a[5],
                    (String) a[6],
                    (String) a[7],
                    (Long) (a[8]),
                    (Long) (a[9]),
                    (Long) (a[10]),
                    (BytesReference) a[11],
                    (PhaseExecutionInfo) a[12]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), MANAGED_BY_ILM_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), POLICY_NAME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LIFECYCLE_DATE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PHASE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ACTION_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), STEP_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FAILED_STEP_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PHASE_TIME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), ACTION_TIME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), STEP_TIME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.copyCurrentStructure(p);
            return BytesArray.bytes(builder);
        }, STEP_INFO_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> PhaseExecutionInfo.parse(p, ""),
            PHASE_EXECUTION_INFO);
    }

    private final String index;
    private final String policyName;
    private final String phase;
    private final String action;
    private final String step;
    private final String failedStep;
    private final Long lifecycleDate;
    private final Long phaseTime;
    private final Long actionTime;
    private final Long stepTime;
    private final boolean managedByILM;
    private final BytesReference stepInfo;
    private final PhaseExecutionInfo phaseExecutionInfo;

    public static IndexLifecycleExplainResponse newManagedIndexResponse(String index, String policyName, Long lifecycleDate,
            String phase, String action, String step, String failedStep, Long phaseTime, Long actionTime, Long stepTime,
            BytesReference stepInfo, PhaseExecutionInfo phaseExecutionInfo) {
        return new IndexLifecycleExplainResponse(index, true, policyName, lifecycleDate, phase, action, step, failedStep, phaseTime,
                actionTime, stepTime, stepInfo, phaseExecutionInfo);
    }

    public static IndexLifecycleExplainResponse newUnmanagedIndexResponse(String index) {
        return new IndexLifecycleExplainResponse(index, false, null, null, null, null, null, null, null, null, null, null, null);
    }

    private IndexLifecycleExplainResponse(String index, boolean managedByILM, String policyName, Long lifecycleDate,
                                          String phase, String action, String step, String failedStep, Long phaseTime, Long actionTime,
                                          Long stepTime, BytesReference stepInfo, PhaseExecutionInfo phaseExecutionInfo) {
        if (managedByILM) {
            if (policyName == null) {
                throw new IllegalArgumentException("[" + POLICY_NAME_FIELD.getPreferredName() + "] cannot be null for managed index");
            }
        } else {
            if (policyName != null || lifecycleDate != null || phase != null || action != null || step != null || failedStep != null
                    || phaseTime != null || actionTime != null || stepTime != null || stepInfo != null || phaseExecutionInfo != null) {
                throw new IllegalArgumentException(
                        "Unmanaged index response must only contain fields: [" + MANAGED_BY_ILM_FIELD + ", " + INDEX_FIELD + "]");
            }
        }
        this.index = index;
        this.policyName = policyName;
        this.managedByILM = managedByILM;
        this.lifecycleDate = lifecycleDate;
        this.phase = phase;
        this.action = action;
        this.step = step;
        this.phaseTime = phaseTime;
        this.actionTime = actionTime;
        this.stepTime = stepTime;
        this.failedStep = failedStep;
        this.stepInfo = stepInfo;
        this.phaseExecutionInfo = phaseExecutionInfo;
    }

    public IndexLifecycleExplainResponse(StreamInput in) throws IOException {
        index = in.readString();
        managedByILM = in.readBoolean();
        if (managedByILM) {
            policyName = in.readString();
            lifecycleDate = in.readOptionalLong();
            phase = in.readOptionalString();
            action = in.readOptionalString();
            step = in.readOptionalString();
            failedStep = in.readOptionalString();
            phaseTime = in.readOptionalLong();
            actionTime = in.readOptionalLong();
            stepTime = in.readOptionalLong();
            stepInfo = in.readOptionalBytesReference();
            phaseExecutionInfo = in.readOptionalWriteable(PhaseExecutionInfo::new);
        } else {
            policyName = null;
            lifecycleDate = null;
            phase = null;
            action = null;
            step = null;
            failedStep = null;
            phaseTime = null;
            actionTime = null;
            stepTime = null;
            stepInfo = null;
            phaseExecutionInfo = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeBoolean(managedByILM);
        if (managedByILM) {
            out.writeString(policyName);
            out.writeOptionalLong(lifecycleDate);
            out.writeOptionalString(phase);
            out.writeOptionalString(action);
            out.writeOptionalString(step);
            out.writeOptionalString(failedStep);
            out.writeOptionalLong(phaseTime);
            out.writeOptionalLong(actionTime);
            out.writeOptionalLong(stepTime);
            out.writeOptionalBytesReference(stepInfo);
            out.writeOptionalWriteable(phaseExecutionInfo);
        }
    }

    public String getIndex() {
        return index;
    }

    public boolean managedByILM() {
        return managedByILM;
    }

    public String getPolicyName() {
        return policyName;
    }

    public Long getLifecycleDate() {
        return lifecycleDate;
    }

    public String getPhase() {
        return phase;
    }

    public Long getPhaseTime() {
        return phaseTime;
    }

    public String getAction() {
        return action;
    }

    public Long getActionTime() {
        return actionTime;
    }

    public String getStep() {
        return step;
    }

    public Long getStepTime() {
        return stepTime;
    }

    public String getFailedStep() {
        return failedStep;
    }

    public BytesReference getStepInfo() {
        return stepInfo;
    }

    public PhaseExecutionInfo getPhaseExecutionInfo() {
        return phaseExecutionInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(MANAGED_BY_ILM_FIELD.getPreferredName(), managedByILM);
        if (managedByILM) {
            builder.field(POLICY_NAME_FIELD.getPreferredName(), policyName);
            if (builder.humanReadable()) {
                builder.field(LIFECYCLE_DATE_FIELD.getPreferredName(), new DateTime(lifecycleDate, ISOChronology.getInstanceUTC()));
            } else {
                builder.field(LIFECYCLE_DATE_FIELD.getPreferredName(), lifecycleDate);
            }
            builder.field(PHASE_FIELD.getPreferredName(), phase);
            if (builder.humanReadable()) {
                builder.field(PHASE_TIME_FIELD.getPreferredName(), new DateTime(phaseTime, ISOChronology.getInstanceUTC()));
            } else {
                builder.field(PHASE_TIME_FIELD.getPreferredName(), phaseTime);
            }
            builder.field(ACTION_FIELD.getPreferredName(), action);
            if (builder.humanReadable()) {
                builder.field(ACTION_TIME_FIELD.getPreferredName(), new DateTime(actionTime, ISOChronology.getInstanceUTC()));
            } else {
                builder.field(ACTION_TIME_FIELD.getPreferredName(), actionTime);
            }
            builder.field(STEP_FIELD.getPreferredName(), step);
            if (builder.humanReadable()) {
                builder.field(STEP_TIME_FIELD.getPreferredName(), new DateTime(stepTime, ISOChronology.getInstanceUTC()));
            } else {
                builder.field(STEP_TIME_FIELD.getPreferredName(), stepTime);
            }
            if (Strings.hasLength(failedStep)) {
                builder.field(FAILED_STEP_FIELD.getPreferredName(), failedStep);
            }
            if (stepInfo != null && stepInfo.length() > 0) {
                builder.rawField(STEP_INFO_FIELD.getPreferredName(), stepInfo.streamInput(), XContentType.JSON);
            }
            if (phaseExecutionInfo != null) {
                builder.field(PHASE_EXECUTION_INFO.getPreferredName(), phaseExecutionInfo);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, managedByILM, policyName, lifecycleDate, phase, action, step, failedStep, phaseTime, actionTime,
                stepTime, stepInfo, phaseExecutionInfo);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        IndexLifecycleExplainResponse other = (IndexLifecycleExplainResponse) obj;
        return Objects.equals(index, other.index) &&
                Objects.equals(managedByILM, other.managedByILM) &&
                Objects.equals(policyName, other.policyName) &&
                Objects.equals(lifecycleDate, other.lifecycleDate) &&
                Objects.equals(phase, other.phase) &&
                Objects.equals(action, other.action) &&
                Objects.equals(step, other.step) &&
                Objects.equals(failedStep, other.failedStep) &&
                Objects.equals(phaseTime, other.phaseTime) &&
                Objects.equals(actionTime, other.actionTime) &&
                Objects.equals(stepTime, other.stepTime) &&
                Objects.equals(stepInfo, other.stepInfo) &&
                Objects.equals(phaseExecutionInfo, other.phaseExecutionInfo);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
