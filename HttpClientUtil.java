package com.yourco.shared.http;

import org.springframework.http.*;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Map;

public class HttpClientUtil {

    private final RestTemplate rest;
    private final HttpHeaders defaultHeaders;

    public HttpClientUtil(int connectTimeoutMs, int readTimeoutMs) {
        HttpComponentsClientHttpRequestFactory rf = new HttpComponentsClientHttpRequestFactory();
        rf.setConnectTimeout(connectTimeoutMs);
        rf.setReadTimeout(readTimeoutMs);
        this.rest = new RestTemplate(rf);

        this.defaultHeaders = new HttpHeaders();
        this.defaultHeaders.setContentType(MediaType.APPLICATION_JSON);
    }

    public <T> ResponseEntity<T> get(String url, Class<T> type) {
        return rest.exchange(url, HttpMethod.GET, new HttpEntity<>(defaultHeaders), type);
    }

    public <T> ResponseEntity<T> get(String url, ParameterizedTypeReference<T> typeRef) {
        return rest.exchange(url, HttpMethod.GET, new HttpEntity<>(defaultHeaders), typeRef);
    }

    public <B, T> ResponseEntity<T> post(String url, B body, Class<T> type) {
        return rest.exchange(url, HttpMethod.POST, new HttpEntity<>(body, defaultHeaders), type);
    }

    public <B, T> ResponseEntity<T> put(String url, B body, Class<T> type) {
        return rest.exchange(url, HttpMethod.PUT, new HttpEntity<>(body, defaultHeaders), type);
    }

    /** Requires HttpComponents request factory, already set above */
    public <B, T> ResponseEntity<T> patch(String url, B body, Class<T> type) {
        return rest.exchange(url, HttpMethod.PATCH, new HttpEntity<>(body, defaultHeaders), type);
    }

    public ResponseEntity<Void> delete(String url) {
        return rest.exchange(url, HttpMethod.DELETE, new HttpEntity<>(defaultHeaders), Void.class);
    }

    /** Optional: add/override headers per call */
    public static HttpHeaders withHeaders(Map<String, String> extra) {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        if (extra != null) extra.forEach(h::add);
        return h;
    }
}
