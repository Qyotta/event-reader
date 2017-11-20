package de.qyotta.eventreader;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
class TestEvent {
   Integer eventId;
   boolean shouldThrowException;
}