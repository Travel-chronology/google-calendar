package by.nexer.googlecalendar.messaging.consumer;

import by.nexer.googlecalendar.messaging.event.TripSendEvent;
import by.nexer.googlecalendar.service.GoogleCalendarService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;


@Component
public class KafkaMessagingConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessagingConsumer.class);
    private static final String topicCreateOrder = "${topic.send-google-calendar}";
    private static final String kafkaConsumerGroupId = "${spring.kafka.consumer.group-id}";
    private final GoogleCalendarService googleCalendarService;

    @Autowired
    public KafkaMessagingConsumer(GoogleCalendarService googleCalendarService) {
        this.googleCalendarService = googleCalendarService;
    }

    @Transactional
    @KafkaListener(topics = topicCreateOrder, groupId = kafkaConsumerGroupId, properties = {"spring.json.value.default.type=by.nexer.googlecalendar.messaging.event.TripSendEvent"})
    public TripSendEvent createGoogleEvent(TripSendEvent tripSendEvent) {
        try {
            googleCalendarService.createEvent(
                    tripSendEvent.getSummary(),
                    tripSendEvent.getDescription(),
                    tripSendEvent.getStartDate(),
                    tripSendEvent.getEndDate());
            logger.info("Event was added in Google calendar");
        } catch (Exception e) {
            logger.warn("Something get wrong with event {}",tripSendEvent);
            throw new RuntimeException("Something get wrong with event " + tripSendEvent);
        }

        return tripSendEvent;
    }

}
