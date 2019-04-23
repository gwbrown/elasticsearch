package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class SnapshotHistoryItem implements Writeable, ToXContentObject {

    protected final String policyId;
    protected final String repository;
    protected final String operation;
    protected final boolean success;

    static final ParseField POLICY_ID = new ParseField("policy");
    static final ParseField REPOSITORY = new ParseField("repository");
    static final ParseField OPERATION = new ParseField("operation");
    static final ParseField SUCCESS = new ParseField("success");

    public SnapshotHistoryItem(String policyId, String repository, String operation, boolean success) {
        this.policyId = Objects.requireNonNull(policyId);
        this.repository = Objects.requireNonNull(repository);
        this.operation = Objects.requireNonNull(operation);
        this.success = success;
    }

    public SnapshotHistoryItem(StreamInput in) throws IOException {
        this.policyId = in.readString();
        this.repository = in.readString();
        this.operation = in.readString();
        this.success = in.readBoolean();
    }

    public String getPolicyId() {
        return policyId;
    }

    public String getRepository() {
        return repository;
    }

    public String getOperation() {
        return operation;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(policyId);
        out.writeString(repository);
        out.writeString(operation);
        out.writeBoolean(success);
        innerWriteTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(POLICY_ID.getPreferredName(), policyId);
            builder.field(REPOSITORY.getPreferredName(), repository);
            builder.field(OPERATION.getPreferredName(), operation);
            builder.field(SUCCESS.getPreferredName(), success);
            innerToXContent(builder, params);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    // NOCOMMIT javadoc
    protected abstract void innerWriteTo(StreamOutput out) throws IOException;

    // NOCOMMIT javadoc
    protected abstract XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotHistoryItem that = (SnapshotHistoryItem) o;
        return isSuccess() == that.isSuccess() &&
            Objects.equals(getPolicyId(), that.getPolicyId()) &&
            Objects.equals(getRepository(), that.getRepository()) &&
            Objects.equals(getOperation(), that.getOperation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPolicyId(), getRepository(), getOperation(), isSuccess());
    }
}
