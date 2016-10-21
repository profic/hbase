package hbase.service;

import hbase.domain.Event;

import java.io.IOException;

/**
 * Created by cloudera on 10/20/16.
 */
public interface EventProcessor {

    void receive(Event event) throws IOException;

}
