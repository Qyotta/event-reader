package de.qyotta.eventreader;

import de.qyotta.eventreader.data.EventReaderState;
import de.qyotta.eventreader.reader.EventReaderRepository;
import de.qyotta.eventreader.reader.EventStore;
import de.qyotta.eventreader.reader.PullEventReader;
import de.qyotta.eventreader.reader.ReadFailedException;
import lombok.Data;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;

@SuppressWarnings("nls")
@RunWith(MockitoJUnitRunner.class)
public class PullEventReaderTest {

   private EventReaderRepository eventReaderRepository = new InMemoryEventReaderRepository();
   @Spy
   private InMemoryEventstore inMemoryEventstore = new InMemoryEventstore();

   private String id = null;

   @Test
   public void shouldNotSkipWhenExceptionOccurs() {
      eventReaderRepository.save(EventReaderState.builder()
            .id("the-id")
            .build());

      final PullEventReader<TestEvent> eventReader = new PullEventReader<TestEvent>(new EventStore<TestEvent>() {

         @Override
         public List<TestEvent> readNextEvents(final String streamName, final String lastHandledEventId, final String eventReaderId) throws ReadFailedException {
            id = lastHandledEventId;
            return Arrays.asList(new TestEvent(1, false), new TestEvent(2, true));
         }

      }, eventReaderRepository, "the-stream", null, "the-id") {

         @Override
         protected String handle(final TestEvent event) {
            if (event.isShouldThrowException()) {
               throw new RuntimeException("booom");
            }
            return String.valueOf(event.getEventId());
         }
      };
      eventReader.start();

      await().atMost(Duration.FIVE_MINUTES)
            .until(() -> assertTrue("1".equals(id) && eventReader.getEventReaderState()
                  .getStackTrace() != null));
   }

   @Test
   public void shouldReadAllEventsOnce() {
      final Set<TestEvent> readEvents = new LinkedHashSet<>();
      final EventStore<TestEvent> eventstore = (String streamName, String lastHandledEventId, final String eventReaderId) -> {
         return inMemoryEventstore.readEvents(Long.valueOf(lastHandledEventId)+1, 4096L);
      };

      final PullEventReader<TestEvent> eventReader = new PullEventReader<TestEvent>(eventstore, eventReaderRepository, "the-stream", null, "the-id") {
         @Override
         protected String handle(final TestEvent event) {
            final boolean unique = readEvents.add(event);
            Assert.assertTrue(unique);
            return String.valueOf(event.getEventId());
         }
      };

      final int numberOfEvents = 100000;
      final int expectedReads = numberOfEvents / 4096;

      inMemoryEventstore.createEvents(numberOfEvents);
      eventReader.start();

      Awaitility.await()
            .atMost(Duration.TEN_SECONDS)
            .until(() -> {
                     Mockito.verify(inMemoryEventstore, Mockito.atLeast(expectedReads))
                           .readEvents(Mockito.anyLong(), Mockito.anyLong());

                     Assert.assertEquals(numberOfEvents, readEvents.size());
                  }
            );
   }

   @Test
   public void shouldReadNewEvents() {
      final List<TestEvent> readEvents = new LinkedList<>();

      final EventStore<TestEvent> eventstore = (streamName, lastHandledEventId, eventReaderId) -> {
         return inMemoryEventstore.readEvents(Long.valueOf(lastHandledEventId) + 1, 4096L);
      };

      final PullEventReader<TestEvent> eventReader = new PullEventReader<TestEvent>(eventstore, eventReaderRepository, "the-stream", null, "the-id") {
         @Override
         protected String handle(final TestEvent event) {
            readEvents.add(event);
            return String.valueOf(event.getEventId());
         }
      };

      eventReader.start();
      inMemoryEventstore.createEvents(10000);

      Awaitility.await()
            .atMost(Duration.TEN_SECONDS)
            .until(() -> Assert.assertEquals(10000, readEvents.size())
            );

      inMemoryEventstore.createEvents(10);

      Awaitility.await()
            .atMost(Duration.TEN_SECONDS)
            .until(() -> Assert.assertEquals(10010, readEvents.size())
            );

      inMemoryEventstore.createEvents(1);

      Awaitility.await()
            .atMost(new Duration(20, TimeUnit.SECONDS))
            .until(() -> Assert.assertEquals(10011, readEvents.size())
            );
   }

   @Test
   @Ignore
   public void shouldRestartAtSameNumberAfterPause() throws Exception {
      final EventStore<TestEvent> eventstore = (streamName, lastHandledEventId, eventReaderId) -> {
         return inMemoryEventstore.readEvents(Long.valueOf(lastHandledEventId), 4096L);
      };

      final PullEventReader<TestEvent> eventReader = new PullEventReader<TestEvent>(eventstore, eventReaderRepository, "the-stream", null, "the-id") {
         @Override
         protected String handle(final TestEvent event) {
            return String.valueOf(event.getEventId());
         }
      };

      inMemoryEventstore.createEvents(1000000);
      eventReader.start();
      Thread.sleep(400);
      eventReader.pause();
      final EventReaderState state = eventReaderRepository.findById("the-id");

      final String lastEventId = state.getEventId();

      eventReader.start();

      Awaitility.await()
            .atMost(Duration.FIVE_SECONDS)
            .until(() -> {
                     Mockito.verify(inMemoryEventstore, Mockito.atLeastOnce())
                           .readEvents(Long.valueOf(lastEventId) + 1, 4096L);
                  }
            );
   }

   @Data
   public class Counter {
      private int value;
   }
}
