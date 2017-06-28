package de.qyotta.eventreader.reader;

import de.qyotta.eventreader.data.EventReaderState;

public interface EventReaderRepository {

   EventReaderState findById(String id);

   void save(EventReaderState eventReaderState);

}
