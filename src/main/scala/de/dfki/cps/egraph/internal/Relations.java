package de.dfki.cps.egraph.internal;

import org.neo4j.graphdb.RelationshipType;

/**
 * @author Martin Ring <martin.ring@dfki.de>
 */
public enum Relations implements RelationshipType {
    NEXT_SIBLING,
    MEMBER
}
