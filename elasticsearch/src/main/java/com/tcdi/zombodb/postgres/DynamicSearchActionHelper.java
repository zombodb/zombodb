package com.tcdi.zombodb.postgres;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.action.search.*;

/**
 * Dynamically figure out which "Search Action" INSTANCE we should use, based on if the SIREn plugin is installed or not
 */
public class DynamicSearchActionHelper {

    public static ClientAction<SearchRequest, SearchResponse, SearchRequestBuilder> getSearchAction() {
        try {
            Class clazz = Class.forName("solutions.siren.join.action.coordinate.CoordinateSearchAction");
            return (ClientAction) clazz.getDeclaredField("INSTANCE").get(clazz);
        } catch (Exception e) {
            return SearchAction.INSTANCE;
        }
    }

    public static ClientAction<MultiSearchRequest, MultiSearchResponse, MultiSearchRequestBuilder> getMultiSearchAction() {
        try {
            Class clazz = Class.forName("solutions.siren.join.action.coordinate.CoordinateMultiSearchAction");
            return (ClientAction) clazz.getDeclaredField("INSTANCE").get(clazz);
        } catch (Exception e) {
            return MultiSearchAction.INSTANCE;
        }
    }
}
