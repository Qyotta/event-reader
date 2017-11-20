package de.qyotta.eventreader;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class InMemoryEventstore {

   private final List<TestEvent> events = new LinkedList<>();

   public List<TestEvent> readEvents(final Long from, final Long limit) {
      return events.stream()
            .filter(testEvent -> testEvent.getEventId() >= from)
            .limit(limit)
            .collect(Collectors.toList());
   }

   public List<TestEvent> createEvents(final int i) {
      final int size = events.size();
      final List<TestEvent> events = IntStream.range(size, size + i)
            .mapToObj(value -> new TestEvent(value, false))
            .collect(Collectors.toList());
      this.events.addAll(events);
      return events;
   }
}
