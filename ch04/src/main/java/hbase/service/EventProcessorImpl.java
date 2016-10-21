package hbase.service;

import hbase.dao.EventHbaseDao;
import hbase.domain.Event;

import java.io.IOException;

/**
 * Created by cloudera on 10/20/16.
 */
public class EventProcessorImpl implements EventProcessor {

    private final EventHbaseDao eventHbaseDao;

    public EventProcessorImpl(EventHbaseDao eventHbaseDao) {
        this.eventHbaseDao = eventHbaseDao;
    }

    @Override
    public void receive(Event event) throws IOException {
        eventHbaseDao.save(event);
    }
}
