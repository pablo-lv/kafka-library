package intg.com.learnkafka.controller;

import com.learnkafka.KafkaLibraryApplication;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = KafkaLibraryApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LibraryEventsControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Test
    void postLibraryEvent() {
        final Book book = Book.builder()
                .bookId(1)
                .bookAuthor("Author")
                .bookName("Title")
                .build();

        final LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> body = new HttpEntity<>(libraryEvent, headers);

        final ResponseEntity<LibraryEvent> response = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, body,
                LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, response.getStatusCode());
    }
}
