package de.qyotta.eventreader.listener;

public interface ExceptionListener {

   void onException(String readerName, String eventId, Throwable throwable, String streamName);

}
