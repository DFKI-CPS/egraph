package de.dfki.cps.egraph;

import org.neo4j.graphdb.Label;

/**
 * @author Martin Ring <martin.ring@dfki.de>
 */
public enum Labels implements Label {
    Resource,
    EObject,
    EReference
}
