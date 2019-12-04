package org.elasticsearch.plugins;

import java.util.function.Predicate;

public interface SystemIndexPlugin {

    /**
     * Returns a predicate that checks whether a name is a system index name for this plugin
     * @return true if the given index name is a system index for this plugin, false otherwise
     */
    default Predicate<String> getSystemIndexPredicate() {
        return (name) -> false;
    }
}
