package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import de.komoot.photon.searcher.TagFilter;

import java.util.*;


/**
 * Collection of query parameters for a search request.
 */
public class PhotonRequest extends PhotonRequestBase {
    private final String query;

    public PhotonRequest(String query, String language) {
        super(language);
        this.query = query;
    }

    public String getQuery() {
        return query;
    }
}
