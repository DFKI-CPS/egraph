package de.dfki.cps.egraph;

import org.neo4j.graphdb.RelationshipType;

/**
 * @author Martin Ring <martin.ring@dfki.de>
 */
public enum Relations implements RelationshipType {
    Contents,
    EReference,
    EReferenceLink,
    Origin,
    OriginResource,
    SContents,
    SLink
}
