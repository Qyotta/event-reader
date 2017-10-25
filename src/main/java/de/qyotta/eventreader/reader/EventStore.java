package de.qyotta.eventreader.reader;

import java.util.List;

public interface EventStore<Event> {
   List<Event> readNextEvents(final String streamName, final String lastHandledEventId) throws ReadFailedException;
}