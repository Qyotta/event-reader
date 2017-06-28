package de.qyotta.eventreader;

import java.util.HashMap;
import java.util.Map;

import de.qyotta.eventreader.data.EventReaderState;
import de.qyotta.eventreader.reader.EventReaderRepository;

final class InMemoryEventReaderRepository implements EventReaderRepository {
   Map<String, EventReaderState> infos = new HashMap<>();

   @Override
   public EventReaderState findById(final String listenerClassName) {
      return infos.get(listenerClassName);
   }

   @Override
   public void save(final EventReaderState eventReaderState) {
      infos.put(eventReaderState.getId(), eventReaderState);

   }
}