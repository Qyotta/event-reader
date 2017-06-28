package de.qyotta.eventreader.reader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.qyotta.eventreader.listener.ExceptionListener;
import io.netty.util.concurrent.DefaultThreadFactory;

@SuppressWarnings("nls")
public abstract class PushEventReader extends EventReader {

   private static final Logger LOGGER = LoggerFactory.getLogger(PushEventReader.class.getName());

   private static final int THROTTLE_TIME_IN_MS = 500;
   private static final int QUEUE_HIGH_MARK = 20000;
   private static final int QUEUE_LOW_MARK = 10000;

   private final AtomicBoolean throttlingEnabled = new AtomicBoolean(false);
   private final AtomicInteger queueSize = new AtomicInteger(0);

   private boolean running = false;
   private boolean wasStopped = false;
   private ExecutorService executorService;

   public PushEventReader(final EventReaderRepository eventReaderRepository, final String streamName, final ExceptionListener exceptionListener, final String id) {
      super(eventReaderRepository, streamName, exceptionListener, id);
   }

   @Override
   protected void startReadingEvents(final String lastHandledEventId) {
      if (running) {
         LOGGER.info("<><><>  [" + getEventReaderId() + "] already running, will do nothing");
         return;
      }
      wasStopped = false;
      running = true;
      createExecutorServiceIfNeeded();

      doSubscribeToStream(lastHandledEventId);

      LOGGER.info("Subscribed to Stream '" + this.streamName + "' for '" + getEventReaderId() + "' beginning from event number: " + lastHandledEventId);
   }

   @Override
   protected void onEventHandled(final String eventId) {
      super.onEventHandled(eventId);
      queueSize.decrementAndGet();
   }

   protected abstract void doSubscribeToStream(final String fromEventNumberExclusive);

   protected void createExecutorServiceIfNeeded() {
      if (executorService != null && !executorService.isShutdown()) {
         return;
      }

      this.executorService = new ScheduledThreadPoolExecutor(1, new DefaultThreadFactory("es-" + getEventReaderId()));
   }

   @Override
   protected void stopReadingEvents() {
      wasStopped = true;
      running = false;
      LOGGER.warn("SHUTDOWN: Stopping subscription from eventstore for '" + getEventReaderId() + "'");
      doStopSubscription();
      queueSize.set(0);
      if (executorService != null) {
         executorService.shutdownNow();
         try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
         } catch (final InterruptedException e) {
            Thread.currentThread()
                  .interrupt();
         }
      }

   }

   protected abstract void doStopSubscription();

   protected void onNewEvent(final EventJob eventJob) {
      if (queueSize.get() > QUEUE_HIGH_MARK) {
         // check if we are above the high-mark
         LOGGER.warn(getEventReaderId() + ": Read queue size reached high-mark (" + QUEUE_HIGH_MARK + "). Will throttle down every " + THROTTLE_TIME_IN_MS + " millis to cool down.");
         throttlingEnabled.set(true);
      } else if (throttlingEnabled.get() && queueSize.get() < QUEUE_LOW_MARK) {
         // check if we are throttling, but below the low-mark
         LOGGER.warn(getEventReaderId() + ": Throttling ended, since queue size for listener reached the low-mark (" + QUEUE_LOW_MARK + ").");
         throttlingEnabled.set(false);
      }

      if (throttlingEnabled.get()) {
         try {
            LOGGER.warn(getEventReaderId() + ": Queue size is " + queueSize.get() + ", so above the low-mark (" + QUEUE_LOW_MARK + "). I will keep cooling down ...");
            Thread.sleep(THROTTLE_TIME_IN_MS);
         } catch (final InterruptedException e) {
            Thread.currentThread()
                  .interrupt();
         }
      }

      queueSize.incrementAndGet();

      try {
         executorService.execute(eventJob);
      } catch (final RejectedExecutionException e) {
         LOGGER.warn("SubscriptionEventJob was rejected by the executor service, maybe the executor service was shutdown before");
      }
   }

   @SuppressWarnings("squid:S1166")
   public abstract class EventJob implements Runnable {

      @Override
      public void run() {
         if (wasStopped) {
            LOGGER.info("<><><>  [" + getEventReaderId() + "] do nothing because job was stopped");
            return;
         }
         try {
            handleEvent();

            onEventHandled();
         } catch (final Exception exception) {
            failedEventReaderInfoWithException(exception);
            stopReadingEvents();
         }
      }

      protected void onEventHandled() {
         PushEventReader.this.onEventHandled(getEventId());

      }

      protected abstract String getEventId();

      protected abstract void handleEvent();

   }

}
