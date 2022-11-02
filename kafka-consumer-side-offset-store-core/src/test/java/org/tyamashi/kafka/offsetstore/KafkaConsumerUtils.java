package org.tyamashi.kafka.offsetstore;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.Heartbeat;
import org.apache.kafka.common.utils.Timer;

import java.lang.reflect.Field;

public class KafkaConsumerUtils {
    private static Field coordinatorField;
    private static Field heartbeatField;
    private static Field sessionTimerField;
    private static Field pollTimerField;

    static {
        // Note: This is robbery viewing private field.
        try {
            coordinatorField = KafkaConsumer.class.getDeclaredField("coordinator");
            coordinatorField.setAccessible(true);
            heartbeatField = AbstractCoordinator.class.getDeclaredField("heartbeat");
            heartbeatField.setAccessible(true);
            sessionTimerField = Heartbeat.class.getDeclaredField("sessionTimer");
            sessionTimerField.setAccessible(true);
            pollTimerField = Heartbeat.class.getDeclaredField("pollTimer");
            pollTimerField.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void expireHeartbeatTime(KafkaConsumer kafkaConsumer) {
        try {
            AbstractCoordinator coordinator = (AbstractCoordinator) coordinatorField.get(kafkaConsumer);
            Heartbeat heartbeat = (Heartbeat) heartbeatField.get(coordinator);
            Timer sessionTimer = (Timer) sessionTimerField.get(heartbeat);
            sessionTimer.resetDeadline(sessionTimer.currentTimeMs());
        } catch (IllegalAccessException e) {
            throw new OffsetStoreValidationException("The coordinator field or the heartbeat field in KafkaConsumer can not be accessed [kafkaConsumer=" + kafkaConsumer + "]");
        }
    }

}
