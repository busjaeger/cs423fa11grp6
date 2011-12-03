package edu.illinois.cs.mapreduce.spi;


public interface LocationPolicy {

    NodeSelectionPolicy getSourcePolicy();

    NodeSelectionPolicy getTargetPolicy();

}
