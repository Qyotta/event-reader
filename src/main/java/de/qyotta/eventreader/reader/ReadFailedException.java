package de.qyotta.eventreader.reader;

public class ReadFailedException extends Exception {

   private static final long serialVersionUID = 1L;

   public ReadFailedException(final Exception e) {
      super(e);
   }

}