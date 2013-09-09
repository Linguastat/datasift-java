package com.datasift.client.managedsource.sources;

import java.util.Map;

/**
 * @author Courtney Robinson <courtney.robinson@datasift.com>
 */
public interface DataSource {
    /**
     * @return A URL encoded version of the parameters returned by {@link #params()}
     */
    String getURLEncoded();

    /**
     * @return a set of parameters for this data source
     */
    Map<String, String> params();
}
