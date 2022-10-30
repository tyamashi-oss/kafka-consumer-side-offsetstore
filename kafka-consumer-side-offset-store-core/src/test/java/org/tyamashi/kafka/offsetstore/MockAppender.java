package org.tyamashi.kafka.offsetstore;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock log4j appender to collect logs in unit test
 */
public class MockAppender extends AbstractAppender {

    List<LogEvent> messages = new ArrayList<LogEvent>();

    protected MockAppender() {
        super("MockAppender", null, null, true, Property.EMPTY_ARRAY);
        start();
    }

    @Override
    public void append(LogEvent event) {
        messages.add(event);
    }

    public boolean isMessageIncluded(String message) {
        return messages.stream().anyMatch(a -> { return a.getMessage().getFormattedMessage().indexOf(message) > -1;});
    }
}
