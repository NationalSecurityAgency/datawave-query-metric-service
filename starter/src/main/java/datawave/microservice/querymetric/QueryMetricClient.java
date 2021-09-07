package datawave.microservice.querymetric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import datawave.microservice.authorization.preauth.ProxiedEntityX509Filter;
import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.microservice.querymetric.config.QueryMetricClientProperties;
import datawave.microservice.querymetric.config.QueryMetricTransportType;
import datawave.microservice.querymetric.function.QueryMetricSupplier;
import datawave.security.authorization.JWTTokenHandler;
import datawave.webservice.result.VoidResponse;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Rest and spring cloud stream client for submitting query metric updates to the query metric service
 *
 * @see Request
 */
public class QueryMetricClient {
    
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private RestTemplate restTemplate;
    
    private QueryMetricClientProperties queryMetricClientProperties;
    
    private QueryMetricSupplier queryMetricSupplier;
    
    private ObjectMapper objectMapper;
    
    private JWTTokenHandler jwtTokenHandler;
    
    public QueryMetricClient(RestTemplateBuilder restTemplateBuilder, QueryMetricClientProperties queryMetricClientProperties,
                    @Autowired(required = false) QueryMetricSupplier queryMetricSupplier, ObjectMapper objectMapper, JWTTokenHandler jwtTokenHandler) {
        this.queryMetricClientProperties = queryMetricClientProperties;
        this.queryMetricSupplier = queryMetricSupplier;
        this.objectMapper = objectMapper;
        this.restTemplate = restTemplateBuilder.build();
        this.jwtTokenHandler = jwtTokenHandler;
    }
    
    public void submit(Request request) throws Exception {
        if (request.metrics == null || request.metrics.isEmpty()) {
            throw new IllegalArgumentException("Request must contain a query metric");
        }
        if (request.metricType == null) {
            throw new IllegalArgumentException("Request must contain a query metric type");
        }
        switch (queryMetricClientProperties.getTransport()) {
            case MESSAGE:
                submitViaMessage(request);
                break;
            default:
                submitViaRest(request);
        }
    }
    
    private void submitViaMessage(Request request) {
        for (BaseQueryMetric metric : request.metrics) {
            QueryMetricUpdate metricUpdate = new QueryMetricUpdate(metric, request.metricType);
            queryMetricSupplier.send(MessageBuilder.withPayload(metricUpdate).build());
        }
    }
    
    private void submitViaRest(Request request) throws Exception {
        if (request.user == null && request.trustedUser == null) {
            throw new IllegalArgumentException("Request must contain either user or trustedUser to use HTTP/HTTPS transport");
        }
        QueryMetricTransportType transportType = queryMetricClientProperties.getTransport();
        if (this.jwtTokenHandler == null) {
            throw new IllegalArgumentException("jwtTokenHandler can not be null with transportType " + transportType.toString());
        }
        QueryMetricType metricType = request.metricType;
        String scheme = transportType.equals(QueryMetricTransportType.HTTPS) ? "https" : "http";
        String host = this.queryMetricClientProperties.getHost();
        int port = this.queryMetricClientProperties.getPort();
        String url;
        Object metricObject;
        if (request.metrics.size() == 1) {
            url = this.queryMetricClientProperties.getUpdateMetricUrl();
            metricObject = request.metrics.get(0);
        } else {
            url = this.queryMetricClientProperties.getUpdateMetricsUrl();
            metricObject = request.metrics;
        }
        
        HttpEntity requestEntity = createRequestEntity(request.user, request.trustedUser, metricObject);
        // @formatter:off
        UriComponents metricUpdateUri = UriComponentsBuilder.newInstance()
                .scheme(scheme)
                .host(host)
                .port(port)
                .path(url)
                .queryParam("metricType", metricType)
                .build();
        // @formatter:on
        restTemplate.postForEntity(metricUpdateUri.toUri(), requestEntity, VoidResponse.class);
    }
    
    protected HttpEntity createRequestEntity(ProxiedUserDetails user, ProxiedUserDetails trustedUser, Object body) throws JsonProcessingException {
        
        HttpHeaders headers = new HttpHeaders();
        if (this.jwtTokenHandler != null && user != null) {
            String token = this.jwtTokenHandler.createTokenFromUsers(user.getUsername(), user.getProxiedUsers());
            headers.add("Authorization", "Bearer " + token);
        }
        if (trustedUser != null) {
            headers.add(ProxiedEntityX509Filter.SUBJECT_DN_HEADER, trustedUser.getPrimaryUser().getDn().subjectDN());
            headers.add(ProxiedEntityX509Filter.ISSUER_DN_HEADER, trustedUser.getPrimaryUser().getDn().issuerDN());
        }
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        return new HttpEntity<>(objectMapper.writeValueAsString(body), headers);
    }
    
    /**
     * Query metric update request
     *
     * @see Request.Builder
     */
    public static class Request {
        
        protected List<BaseQueryMetric> metrics;
        protected QueryMetricType metricType;
        protected ProxiedUserDetails user;
        protected ProxiedUserDetails trustedUser;
        
        private Request() {}
        
        /**
         * Constructs a query metric update request
         *
         * @param b
         *            {@link Builder} for the query metric update request
         */
        protected Request(Builder b) {
            this.metrics = b.metrics;
            this.metricType = b.metricType;
            this.user = b.user;
            this.trustedUser = b.trustedUser;
        }
        
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this).toString();
        }
        
        /**
         * Builder for base audit requests
         */
        public static class Builder {
            
            protected List<BaseQueryMetric> metrics;
            protected QueryMetricType metricType;
            protected ProxiedUserDetails user;
            protected ProxiedUserDetails trustedUser;
            
            public Builder withMetricType(QueryMetricType metricType) {
                this.metricType = metricType;
                return this;
            }
            
            public Builder withMetric(BaseQueryMetric metric) {
                this.metrics = Collections.singletonList(metric);
                return this;
            }
            
            public Builder withMetrics(List<BaseQueryMetric> metrics) {
                this.metrics = metrics;
                return this;
            }
            
            public Builder withUser(ProxiedUserDetails user) {
                this.user = user;
                return this;
            }
            
            public Builder withTrustedUser(ProxiedUserDetails trustedUser) {
                this.trustedUser = trustedUser;
                return this;
            }
            
            public Request build() {
                return new Request(this);
            }
        }
    }
}
