package com.datasift.client.historics;

import com.datasift.client.DataSiftApiClient;
import com.datasift.client.DataSiftConfig;
import com.datasift.client.DataSiftResult;
import com.datasift.client.FutureData;
import io.higgs.http.client.POST;
import io.higgs.http.client.future.PageReader;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * This class provides access to the DataSift Historics API.
 */
public class DataSiftHistorics extends DataSiftApiClient {
    public final String PREPARE = "historics/prepare", START = "historics/start", STOP = "historics/stop",
            UPDATE = "historics/update", STATUS = "historics/status", DELETE = "historics/delete",
            GET = "historics/get";

    public DataSiftHistorics(DataSiftConfig config) {
        super(config);
    }

    /**
     * Start the historics query with the given ID
     *
     * @param id the historics id
     * @return a result which can be checked for success or failure, A status 204 indicates success,
     *         or using {@link com.datasift.client.DataSiftResult#isSuccessful()}
     */
    public FutureData<DataSiftResult> start(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("A valid ID is required to start a Historics query");
        }
        FutureData<DataSiftResult> future = new FutureData<DataSiftResult>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(START));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new DataSiftResult())))
                .form("id", id);
        applyConfig(request).execute();
        return future;
    }

    /**
     * Stop a given historics query
     *
     * @param id     the historics ID
     * @param reason an optional ID
     * @return
     */
    public FutureData<DataSiftResult> stop(String id, String reason) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("A valid ID is required to stop a Historics query");
        }
        FutureData<DataSiftResult> future = new FutureData<DataSiftResult>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(STOP));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new DataSiftResult())))
                .form("id", id);
        if (reason != null) {
            request.form("reason", reason);
        }
        applyConfig(request).execute();
        return future;
    }

    /**
     * Delete the historic with the given ID
     *
     * @param id an historic ID
     * @return a result indicating whether the request was successful or not
     */
    public FutureData<DataSiftResult> delete(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("A valid ID is required to delete a Historics query");
        }
        FutureData<DataSiftResult> future = new FutureData<DataSiftResult>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(DELETE));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new DataSiftResult())))
                .form("id", id);
        applyConfig(request).execute();
        return future;
    }

    /**
     * Update the name of a historics query
     *
     * @param id   the ID of the historics to update
     * @param name the new name for the historics
     * @return a result that can be used to check the success or failure of the request
     */
    public FutureData<DataSiftResult> update(String id, String name) {
        if (id == null || name == null || id.isEmpty() || name.isEmpty()) {
            throw new IllegalArgumentException("A valid ID AND name is required to update a Historics query");
        }
        FutureData<DataSiftResult> future = new FutureData<DataSiftResult>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(UPDATE));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new DataSiftResult())))
                .form("id", id)
                .form("name", name);
        applyConfig(request).execute();
        return future;
    }

    public FutureData<HistoricsStatus> status(DateTime start, DateTime end) {
        return status(start, end, null);
    }

    /**
     * Check the status of data availability in our archive for the given time period
     *
     * @param start   the dat from which the archive should be checked
     * @param end     the up to which the archive should be checked
     * @param sources an optional list of data sources that should be queried, e.g. [facebook,twitter,...]
     * @return a report of the current status/availability of data for the given time period
     */
    public FutureData<HistoricsStatus> status(DateTime start, DateTime end, String... sources) {
        FutureData<HistoricsStatus> future = new FutureData<HistoricsStatus>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(STATUS));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new HistoricsStatus())))
                .form("start", TimeUnit.MILLISECONDS.toSeconds(start.getMillis()))
                .form("end", TimeUnit.MILLISECONDS.toSeconds(end.getMillis()));
        if (sources != null && sources.length > 0) {
            StringBuilder b = new StringBuilder();
            for (String source : sources) {
                b.append(source).append(",");
            }
            request.form("sources", b.toString().substring(0, b.length() - 1));
        }
        applyConfig(request).execute();
        return future;
    }

    public FutureData<HistoricsQuery> get(String id) {
        return get(id, true);
    }

    /**
     * Get detailed information about a historics query
     *
     * @param id           the id of the historics to get
     * @param withEstimate if true then an estimated completion time is include in the response
     * @return a historics query
     */
    public FutureData<HistoricsQuery> get(String id, boolean withEstimate) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("ID is required get a historics query");
        }
        FutureData<HistoricsQuery> future = new FutureData<HistoricsQuery>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(GET));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new HistoricsQuery())))
                .form("id", id)
                .form("with_estimate", withEstimate ? 1 : 0);
        applyConfig(request).execute();
        return future;
    }

    /**
     * Retrieve a list of {@link HistoricsQuery} objects
     *
     * @param max          max number of objects to get
     * @param page         a page number
     * @param withEstimate if true, include an estimated completion time
     * @return an iterable list of {@link HistoricsQuery}s
     */
    public FutureData<HistoricsQueryList> get(int max, int page, boolean withEstimate) {
        FutureData<HistoricsQueryList> future = new FutureData<HistoricsQueryList>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(GET));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new HistoricsQueryList())))
                .form("with_estimate", withEstimate ? 1 : 0);
        if (max > 0) {
            request.form("max", max);
        }
        if (page > 0) {
            request.form("page", page);
        }
        applyConfig(request).execute();
        return future;
    }

    /**
     * @param hash    The hash of the CSDL for your playback query.
     *                Example values: 2459b03a13577579bca76471778a5c3d
     * @param start   Unix timestamp for the start time.
     *                Example values: 1325548800
     * @param end     nix timestamp for the end time. Must be at least 24 in the past.
     *                Example values: 1325548800
     * @param name    The name you assign to your playback query.
     *                Example values: Football
     * @param sources Comma-separated list of data sources to include. Currently,
     *                the only source you can use is twitter. In the future, you will be able to choose any source
     *                listed as a valid value that you would use in the interaction.type target.
     *                Example values: twitter
     * @return the prepared playback
     */
    public FutureData<PreparedHistoricsQuery> prepare(String hash, long start, long end, String name,
                                                      String... sources) {
        return prepare(hash, start, end, name, -1, sources);
    }

    public FutureData<PreparedHistoricsQuery> prepare(String hash, long start, long end, String name, int sample,
                                                      String... sources) {
        FutureData<PreparedHistoricsQuery> future = new FutureData<PreparedHistoricsQuery>();
        URI uri = newParams().forURL(config.newAPIEndpointURI(PREPARE));
        POST request = config.http().POST(uri, new PageReader(newRequestCallback(future, new PreparedHistoricsQuery())))
                .form("hash", hash)
                .form("start", start)
                .form("end", end)
                .form("name", name);
        if (sample > 0) {
            request.form("sample", sample);
        }
        if (sources != null && sources.length > 0) {
            StringBuilder b = new StringBuilder();
            for (String source : sources) {
                b.append(source).append(",");
            }
            request.form("sources", b.toString().substring(0, b.length() - 1));
        }
        applyConfig(request).execute();
        return future;
    }
}
