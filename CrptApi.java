package com.shoggoth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CrptApi {
    private static final String API_URI = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private static final Logger LOGGER = Logger.getLogger(CrptApi.class.getName());
    private final TimeUnit timeUnit;
    private final Integer requestsPerTimeUnitLimit;
    private final Semaphore semaphore;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public CrptApi(TimeUnit timeUnit, Integer requestsPerTimeUnitLimit) {
        this.timeUnit = timeUnit;
        this.requestsPerTimeUnitLimit = requestsPerTimeUnitLimit;
        this.semaphore = new Semaphore(requestsPerTimeUnitLimit, true);
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
        startScheduledExecutor();
    }

    public void createDocument(Document document, String signature) {
        semaphore.acquireUninterruptibly();
        ApiRequest request = new ApiRequest(document, signature);
        sendApiRequest(request);

    }

    private void startScheduledExecutor() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                this::releaseSemaphorePermits,
                0,
                1,
                timeUnit
        );
    }

    private void releaseSemaphorePermits() {
        int acquiredPermits = requestsPerTimeUnitLimit - semaphore.availablePermits();
        semaphore.release(acquiredPermits);
    }

    private void sendApiRequest(ApiRequest apiRequest) {
        try {
            String requestBody = objectMapper.writeValueAsString(apiRequest);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(API_URI))
                    .header("Content-type", MediaType.APPLICATION_JSON)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (URISyntaxException | IOException | InterruptedException e) {
            LOGGER.log(Level.INFO, "IN sendApiRequest: " + e.getMessage());
        }
    }

    public record ApiRequest(
            Document document,
            String signature
    ) {
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record Document(
            Description description,
            String docId,
            String docStatus,
            DocumentType docType,
            @JsonProperty("importRequest")
            boolean importRequest,
            String participantInn,
            String ownerInn,
            String producerInn,
            LocalDate productionDate,
            String productionType,
            List<Product> products,
            LocalDate regDate,
            String reg_number
    ) {
    }

    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record Product(
            String certificateDocument,
            LocalDate certificateDocumentDate,
            String certificateDocumentNumber,
            String ownerInn,
            String producerInn,
            LocalDate productionDate,
            String tnvedCode,
            String uitCode,
            String uituCode
    ) {
    }

    public record Description(
            String participantInn
    ) {
    }

    enum DocumentType {
        LP_INTRODUCE_GOODS
    }
}
