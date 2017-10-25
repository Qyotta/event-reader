package de.qyotta.eventreader;

import static com.jayway.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.jayway.awaitility.Duration;

import de.qyotta.eventreader.data.EventReaderState;
import de.qyotta.eventreader.reader.EventReaderRepository;
import de.qyotta.eventreader.reader.EventStore;
import de.qyotta.eventreader.reader.PullEventReader;
import de.qyotta.eventreader.reader.ReadFailedException;
import lombok.AllArgsConstructor;
import lombok.Data;

@SuppressWarnings("nls")
@RunWith(MockitoJUnitRunner.class)
public class PullEventReaderTest {

   EventReaderRepository eventReaderRepository = new InMemoryEventReaderRepository();

   String id = null;

   @Test
   public void doSkipWhenExceptionOccurs() throws Exception {
      eventReaderRepository.save(EventReaderState.builder()
            .id("the-id")
            .build());

      final PullEventReader<TestEvent> eventReader = new PullEventReader<TestEvent>(new EventStore<TestEvent>() {

         @Override
         public List<TestEvent> readNextEvents(final String streamName, final String lastHandledEventId) throws ReadFailedException {
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

   @Data
   @AllArgsConstructor
   class TestEvent {
      Integer eventId;
      boolean shouldThrowException;
   }
}
