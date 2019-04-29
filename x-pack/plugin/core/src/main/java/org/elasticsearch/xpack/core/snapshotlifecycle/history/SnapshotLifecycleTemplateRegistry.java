package org.elasticsearch.xpack.core.snapshotlifecycle.history;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.INDEX_LIFECYCLE_ORIGIN;

public class SnapshotLifecycleTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final String INDEX_TEMPLATE_VERSION = "1";

    public static final String SLM_TEMPLATE_VERSION_VARIABLE = "xpack.slm.template.version";
    public static final String SLM_TEMPLATE_NAME = ".slm-history";

    public static final String SLM_POLICY_NAME = "slm-history-ilm-policy";

    public static final IndexTemplateConfig TEMPLATE_SLM_HISTORY = new IndexTemplateConfig(
        SLM_TEMPLATE_NAME,
        "/slm-history.json",
        INDEX_TEMPLATE_VERSION,
        SLM_TEMPLATE_VERSION_VARIABLE
    );

    public static final LifecyclePolicyConfig SLM_HISTORY_POLICY = new LifecyclePolicyConfig(
        SLM_POLICY_NAME,
        "/slm-history-ilm-policy.json"
    );

    public SnapshotLifecycleTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool, Client client,
                                             NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
        return Collections.singletonList(TEMPLATE_SLM_HISTORY);
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return Collections.singletonList(SLM_HISTORY_POLICY);
    }

    @Override
    protected String getOrigin() {
        return INDEX_LIFECYCLE_ORIGIN; // TODO use separate SLM origin?
    }
}
