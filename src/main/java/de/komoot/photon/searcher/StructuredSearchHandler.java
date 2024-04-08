package de.komoot.photon.searcher;

import de.komoot.photon.StructuredPhotonRequest;
import de.komoot.photon.query.PhotonRequest;

import java.util.List;

public interface StructuredSearchHandler {
    List<PhotonResult> search(StructuredPhotonRequest photonRequest);
}
