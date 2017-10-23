package de.qyotta.eventreader.reader;

import java.util.List;

public interface EventStore<Event> {
   List<Event> readNextEvents(final String lastHandledEventId) throws ReadFailedException;

   String getStream();
}