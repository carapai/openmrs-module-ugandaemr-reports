package org.openmrs.module.ugandaemrreports.common;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by carapai on 28/10/2017.
 */
public class DataProcessor {

    private List<Data> data;

    public DataProcessor() {
    }

    public DataProcessor(List<Data> data) {
        this.data = data;

    }

    public List<Data> getData() {
        return data;
    }

    public void setData(List<Data> data) {
        this.data = data;
    }

    public DataProcessor filter(Function<Data, Boolean> function) {
        this.data = this.data.stream().filter(function::apply).collect(Collectors.toList());
        return this;
    }
}
