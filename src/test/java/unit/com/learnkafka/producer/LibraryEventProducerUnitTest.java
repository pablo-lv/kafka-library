package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Test
    void sendLibraryEventApproach2_failure() throws JsonProcessingException, ExecutionException, InterruptedException {
        final Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Author")
                .bookName("Title")
                .build();

        final LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture feature = new SettableListenableFuture();
        feature.setException(new RuntimeException("Exception calling kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(feature);
        assertThrows(
                Exception.class, () ->
                libraryEventProducer.sendLibraryEventApproach2(libraryEvent).get()
        );
    }

    @Test
    void sendLibraryEventApproach2_success() throws JsonProcessingException, ExecutionException, InterruptedException {
        final Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Author")
                .bookName("Title")
                .build();

        final LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture feature = new SettableListenableFuture();

        String  record = objectMapper.writeValueAsString(libraryEvent.getBook());
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), record);
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition("library-events", 1),
                1, 1, 342, System.currentTimeMillis(), 1, 2
        );
        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);
        feature.set(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(feature);


        ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEventApproach2(libraryEvent);

        //then
        SendResult<Integer, String> sendResult1 = listenableFuture.get();
        assert sendResult1.getRecordMetadata().partition() == 1;
    }
}
