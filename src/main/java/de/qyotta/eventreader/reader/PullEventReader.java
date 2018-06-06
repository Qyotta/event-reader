package de.qyotta.eventreader.reader;

import de.qyotta.eventreader.listener.ExceptionListener;
import de.qyotta.eventreader.util.MetricConstants;
import io.prometheus.client.Gauge;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("nls")
public abstract class PullEventReader<T> extends EventReader {

   private static final Logger LOGGER = LoggerFactory.getLogger(PullEventReader.class);
   private static final int DEFAULT_INITIAL_DELAY = 1;

   private static final Gauge DELAY_GAUGE = Gauge.build()
         .namespace("event_reader")
         .name("pull_delay")
         .help("eventreader scheduled delay")
         .labelNames("stream", "id", "type")
         .register();

   private static final Gauge SCHEDULER_GAUGE = Gauge.build()
         .namespace("event_reader")
         .name("pull_heartbeat")
         .help("eventreader last execution")
         .labelNames("stream", "id", "point", "type")
         .register();

   private int delay;
   private int initialDelay = 1;
   private final Runnable eventProcessingRun = new EventProcessingRun();
   private ScheduledExecutorService newScheduledThreadPool;

   private boolean wasStopped;

   private final EventStore<T> eventStore;

   public PullEventReader(final EventStore<T> eventStore, final EventReaderRepository eventReaderRepository, final String streamName, final ExceptionListener exceptionListener, final String id,
         final int initialDelay) {
      super(eventReaderRepository, streamName, exceptionListener, id);
      this.eventStore = eventStore;
      this.initialDelay = initialDelay;
      resetDelay();
   }

   public PullEventReader(final EventStore<T> eventStore, final EventReaderRepository eventReaderRepository, final String streamName, final ExceptionListener exceptionListener, final String id) {
      this(eventStore, eventReaderRepository, streamName, exceptionListener, id, DEFAULT_INITIAL_DELAY);
   }

   private void resetDelay() {
      this.delay = initialDelay;
      DELAY_GAUGE.labels(streamName, getEventReaderId(), MetricConstants.PULL)
            .set(delay / 1000.d);
   }

   private void doubleDelay() {
      delay = delay * 2;
      DELAY_GAUGE.labels(streamName, getEventReaderId(), MetricConstants.PULL)
            .set(delay / 1000.d);
   }

   /**
    * @param fromEventNumberExclusive
    *           No used here, because we want to control what we pull
    */
   @Override
   protected void startReadingEvents(final String fromEventNumberExclusive) {
      LOGGER.debug("[" + getEventReaderId() + "] Starting reading events from " + fromEventNumberExclusive);
      resetDelay();
      wasStopped = false;
      newScheduledThreadPool = Executors.newSingleThreadScheduledExecutor();
      scheduleNextRun();
   }

   protected void processEvents() {
      try {
         if (wasStopped) {
            return;
         }

         final String lastHandledEventId = minusOneIfEventIdIsNotSet();

         final List<T> events = eventStore.readNextEvents(streamName, lastHandledEventId, getEventReaderId());

         LOGGER.debug("[" + getEventReaderId() + "] Found " + events.size() + " events");

         for (final T eventResponse : events) {
            if (wasStopped) {
               return;
            }

            final String handledEventId = handle(eventResponse);
            onEventHandled(handledEventId);
            resetDelay();
         }

         LOGGER.debug("[" + getEventReaderId() + "] Handled all events from response");
      } catch (final Exception e) {
         LOGGER.error(e.getMessage(), e);
         runningEventReaderInfoWithException(e);
         doubleDelay();
      }
   }

   protected abstract String handle(T event);

   @Override
   protected void stopReadingEvents() {
      wasStopped = true;
      if (newScheduledThreadPool != null) {
         newScheduledThreadPool.shutdown();
         try {
            newScheduledThreadPool.awaitTermination(30, TimeUnit.SECONDS);
         } catch (final InterruptedException e) {
            Thread.currentThread()
                  .interrupt();
         }
      }
   }

   private String minusOneIfEventIdIsNotSet() {
      final String eventId = getEventReaderState().getEventId();
      if (eventId == null) {
         return "-1";
      }
      return eventId;
   }

   private double getCurrentSeconds() {
      final long currentTimeMillis = System.currentTimeMillis();
      return currentTimeMillis / 1000.0d;
   }

   private void scheduleNextRun() {
      LOGGER.debug("[" + getEventReaderId() + "] Scheduling next run in " + delay);
      newScheduledThreadPool.schedule(eventProcessingRun, delay, TimeUnit.MILLISECONDS);
      SCHEDULER_GAUGE.labels(streamName, getEventReaderId(), MetricConstants.SCHEDULE, MetricConstants.PULL)
            .set(getCurrentSeconds());
   }

   private void beforeProcessing() {
      SCHEDULER_GAUGE.labels(streamName, getEventReaderId(), MetricConstants.START, MetricConstants.PULL)
            .set(getCurrentSeconds());
   }

   private void afterProcessing() {
      SCHEDULER_GAUGE.labels(streamName, getEventReaderId(), MetricConstants.END, MetricConstants.PULL)
            .set(getCurrentSeconds());
   }

   private final class EventProcessingRun implements Runnable {
      @Override
      public void run() {
         LOGGER.debug("[" + getEventReaderId() + "] Scheduled event processing");
         beforeProcessing();
         processEvents();
         afterProcessing();
         scheduleNextRun();
      }

   }
}