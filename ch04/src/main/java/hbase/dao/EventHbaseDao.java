package hbase.dao;

import hbase.domain.Event;

import java.io.IOException;

/**
 * Created by cloudera on 10/20/16.
 */
public interface EventHbaseDao {
    void save(Event event) throws IOException;
}
