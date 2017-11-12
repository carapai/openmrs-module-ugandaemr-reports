package org.openmrs.module.ugandaemrreports.common;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by carapai on 28/10/2017.
 */
public class ObsProcessor {

    private List<SummarizedObs> summarizedObs;

    public ObsProcessor() {
    }

    public ObsProcessor(List<SummarizedObs> summarizedObs) {
        this.summarizedObs = summarizedObs;

    }

    public List<SummarizedObs> getSummarizedObs() {
        return summarizedObs;
    }

    public void setSummarizedObs(List<SummarizedObs> summarizedObs) {
        this.summarizedObs = summarizedObs;
    }

    public ObsProcessor filter(Function<SummarizedObs, Boolean> function) {
        this.summarizedObs = summarizedObs.stream().filter(function::apply).collect(Collectors.toList());
        return this;
    }
}
