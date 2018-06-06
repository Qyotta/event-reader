package de.qyotta.eventreader.reader;

import de.qyotta.eventreader.data.EventReaderState;
import de.qyotta.eventreader.listener.ExceptionListener;
import io.prometheus.client.Gauge;

import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("nls")
public abstract class EventReader {

   private static final Logger LOGGER = LoggerFactory.getLogger(EventReader.class.getName());

   private static final String LISTENER_LABEL = "listener";

   private static final String STREAM_LABEL = "stream";

   private static final Gauge eventIdGauge = Gauge.build()
         .namespace("event_reader")
         .name("eventid")
         .labelNames(LISTENER_LABEL, STREAM_LABEL)
         .help("EventId for eventreader")
         .register();

   private final EventReaderRepository eventReaderRepository;

   private final ExceptionListener exceptionListener;

   protected final String streamName;

   private final String eventReaderId;

   public EventReader(final EventReaderRepository eventReaderRepository, final String streamName, final ExceptionListener exceptionListener, final String eventReaderId) {
      this.eventReaderRepository = eventReaderRepository;
      this.streamName = streamName;
      this.exceptionListener = exceptionListener;
      this.eventReaderId = eventReaderId;
   }

   public void stop() throws TimeoutException {
      stopReadingEvents();
      persistStopState();
   }

   public void pause() throws TimeoutException {
      stopReadingEvents();
      persistPauseState();
   }

   public void shutdown() {
      stopReadingEvents();
   }

   public void start() {
      LOGGER.debug("<><><> [" + getEventReaderId() + "] start()");
      stopReadingEvents();
      persistStartState();
      run();
   }

   public synchronized void run() {
      final EventReaderState eventReaderState = eventReaderRepository.findById(eventReaderId);

      if (eventReaderState == null) {
         LOGGER.warn("Could not find reader info with id '" + eventReaderId
               + ". Will >> NOT << start the reader from the very first event to prevent you from harm. You might be distracted by this, but think about the moment when it saves you.  If you really want to start the reader from the first event, please explicitly crate the document in the collection.");
         return;
      }

      LOGGER.info("<><><> [" + getEventReaderId() + "] found eventReaderInfo " + eventReaderState.getId() + "@" + eventReaderState.getEventId() + " state: " + eventReaderState.getState());

      if (eventReaderState.getEventId() != null) {
         setEventReaderEventId(eventReaderId, streamName, eventReaderState.getEventId());
      }

      if (eventReaderState.getState() == null) {
         startReadingEvents(eventReaderState.getEventId());
         return;
      }

      switch (eventReaderState.getState()) {
         case STOPPED:
         case PAUSED:
         case FAILED:
            return;
         case RUNNING:
         default:
            startReadingEvents(eventReaderState.getEventId());
            return;
      }
   }

   protected abstract void startReadingEvents(String lastHandledEventId);

   public String getEventReaderId() {
      return eventReaderId;
   }

   public EventReaderState getEventReaderState() {
      final EventReaderState eventReaderState = eventReaderRepository.findById(eventReaderId);
      if (eventReaderState == null) {
         return EventReaderState.builder()
               .id(eventReaderId)
               .build();
      }
      return eventReaderState;
   }

   private void persistStartState() {
      eventReaderRepository.save(getEventReaderState().toBuilder()
            .state(null)
            .stackTrace(null)
            .build());
   }

   private void persistStopState() {
      eventReaderRepository.save(EventReaderState.builder()
            .id(eventReaderId)
            .eventId(null)
            .state(EventReaderState.State.STOPPED)
            .stackTrace(null)
            .build());
   }

   private void persistPauseState() {
      eventReaderRepository.save(getEventReaderState().toBuilder()
            .state(EventReaderState.State.PAUSED)
            .build());
   }

   protected void onEventHandled(final String eventId) {
      LOGGER.debug("[" + getEventReaderId() + "] Handled event " + eventId);
      eventReaderRepository.save(EventReaderState.builder()
            .id(this.eventReaderId)
            .eventId(eventId)
            .state(EventReaderState.State.RUNNING)
            .stackTrace(null)
            .build());
      setEventReaderEventId(this.eventReaderId, streamName, eventId);
   }

   protected abstract void stopReadingEvents();

   protected void failedEventReaderInfoWithException(final Throwable throwable) {
      final EventReaderState eventReaderState = getEventReaderState();
      eventReaderRepository.save(eventReaderState.toBuilder()
            .state(EventReaderState.State.FAILED)
            .stackTrace(throwable.getMessage() + ":\n" + ExceptionUtils.getStackTrace(throwable))
            .build());
      if (exceptionListener != null) {
         exceptionListener.onException(eventReaderId, eventReaderState.getEventId(), throwable, streamName);
      }
   }

   protected void runningEventReaderInfoWithException(final Throwable throwable) {
      final EventReaderState eventReaderInfo = getEventReaderState();
      eventReaderRepository.save(eventReaderInfo.toBuilder()
            .state(EventReaderState.State.RUNNING)
            .stackTrace(throwable.getMessage() + ":\n" + ExceptionUtils.getStackTrace(throwable))
            .build());
   }

   public void init() {
      persistStartState();
      run();
   }

   private static void setEventReaderEventId(final String listenerClassName, final String streamName, final String eventId) {
      eventIdGauge.labels(listenerClassName, streamName)
            .set(Double.valueOf(eventId.split("@")[0]));
   }

}