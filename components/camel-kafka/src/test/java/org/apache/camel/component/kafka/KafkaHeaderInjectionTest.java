/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.kafka;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultHeaderFilterStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The exchange headers consulted by the Kafka producer must live in the Camel header namespace so that the default
 * transport header filter strategies (which only filter the {@code Camel} prefix, such as the HTTP one) strip them when
 * they are sent by an untrusted peer of a route bridged to a Kafka producer.
 */
public class KafkaHeaderInjectionTest {

    private static final String TOPIC = "configured-topic";
    private static final String ATTACKER_TOPIC = "attacker-topic";
    private static final String ATTACKER_KEY = "attacker-key";

    private CamelContext context;
    private MockProducer<String, String> mockProducer;
    private KafkaProducer camelProducer;

    @BeforeEach
    public void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();

        KafkaComponent component = new KafkaComponent(context);
        KafkaEndpoint endpoint = (KafkaEndpoint) component.createEndpoint("kafka:" + TOPIC);

        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        camelProducer = new KafkaProducer(endpoint);
        camelProducer.setKafkaProducer(mockProducer);
    }

    @AfterEach
    public void tearDown() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    public void kafkaHeaderConstantsAreFilteredAtTransportBoundary() {
        Exchange exchange = new DefaultExchange(context);
        HeaderFilterStrategy transportFilter = transportBoundaryFilter();
        HeaderFilterStrategy kafkaFilter = new KafkaHeaderFilterStrategy();

        List<String> headers = Arrays.asList(KafkaConstants.PARTITION_KEY, KafkaConstants.PARTITION, KafkaConstants.KEY,
                KafkaConstants.TOPIC, KafkaConstants.OVERRIDE_TOPIC, KafkaConstants.OFFSET, KafkaConstants.HEADERS,
                KafkaConstants.LAST_RECORD_BEFORE_COMMIT, KafkaConstants.LAST_POLL_RECORD, KafkaConstants.TIMESTAMP,
                KafkaConstants.OVERRIDE_TIMESTAMP, KafkaConstants.MANUAL_COMMIT);

        for (String header : headers) {
            assertTrue(header.startsWith("Camel"), "Kafka header " + header + " must be in the Camel namespace");
            assertTrue(transportFilter.applyFilterToExternalHeaders(header, "value", exchange),
                    "Kafka header " + header + " must be filtered by the default transport header filter strategy");
            assertTrue(kafkaFilter.applyFilterToExternalHeaders(header, "value", exchange),
                    "Kafka header " + header + " must be filtered by the Kafka header filter strategy");
        }
    }

    @Test
    public void producerHeadersInjectedByUntrustedPeerAreStripped() throws Exception {
        Map<String, Object> external = new LinkedHashMap<>();
        external.put(KafkaConstants.OVERRIDE_TOPIC, ATTACKER_TOPIC);
        external.put(KafkaConstants.KEY, ATTACKER_KEY);
        external.put(KafkaConstants.PARTITION_KEY, 7);
        external.put(KafkaConstants.OVERRIDE_TIMESTAMP, 0L);

        Exchange exchange = receiveFromUntrustedPeer(external);
        camelProducer.process(exchange);

        assertRecordNotTampered();
    }

    @Test
    public void legacyKafkaHeaderNamesAreNotHonouredByProducer() throws Exception {
        Map<String, Object> external = new LinkedHashMap<>();
        external.put("kafka.OVERRIDE_TOPIC", ATTACKER_TOPIC);
        external.put("kafka.KEY", ATTACKER_KEY);
        external.put("kafka.PARTITION_KEY", 7);
        external.put("kafka.OVERRIDE_TIMESTAMP", 0L);

        Exchange exchange = receiveFromUntrustedPeer(external);
        // the legacy names are outside the Camel namespace and pass the transport boundary
        assertEquals(ATTACKER_TOPIC, exchange.getIn().getHeader("kafka.OVERRIDE_TOPIC"));

        camelProducer.process(exchange);

        assertRecordNotTampered();
    }

    @Test
    public void overrideTopicSetByRouteIsStillHonoured() throws Exception {
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("payload");
        exchange.getIn().setHeader(KafkaConstants.OVERRIDE_TOPIC, "routed-topic");
        exchange.getIn().setHeader(KafkaConstants.KEY, "routed-key");

        camelProducer.process(exchange);

        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size());
        assertEquals("routed-topic", history.get(0).topic());
        assertEquals("routed-key", history.get(0).key());
        assertNull(exchange.getIn().getHeader(KafkaConstants.OVERRIDE_TOPIC), "Override topic header is not preserved");
    }

    private void assertRecordNotTampered() {
        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size());
        ProducerRecord<String, String> record = history.get(0);
        assertEquals(TOPIC, record.topic(), "Untrusted peer must not be able to redirect the record to another topic");
        assertNull(record.key(), "Untrusted peer must not be able to set the record key");
        assertNull(record.partition(), "Untrusted peer must not be able to set the record partition");
        assertNull(record.timestamp(), "Untrusted peer must not be able to set the record timestamp");
        assertEquals("payload", record.value());
    }

    /**
     * Inbound header filter strategy used by transport components such as HTTP, filtering the Camel namespace only.
     */
    private static HeaderFilterStrategy transportBoundaryFilter() {
        DefaultHeaderFilterStrategy strategy = new DefaultHeaderFilterStrategy();
        strategy.setLowerCase(true);
        strategy.setInFilterStartsWith(DefaultHeaderFilterStrategy.CAMEL_FILTER_STARTS_WITH);
        return strategy;
    }

    /**
     * Simulates a transport consumer (for example HTTP) mapping the headers of an untrusted request onto the exchange.
     */
    private Exchange receiveFromUntrustedPeer(Map<String, Object> externalHeaders) {
        HeaderFilterStrategy strategy = transportBoundaryFilter();
        Exchange exchange = new DefaultExchange(context);
        exchange.getIn().setBody("payload");
        for (Map.Entry<String, Object> entry : externalHeaders.entrySet()) {
            if (!strategy.applyFilterToExternalHeaders(entry.getKey(), entry.getValue(), exchange)) {
                exchange.getIn().setHeader(entry.getKey(), entry.getValue());
            }
        }
        return exchange;
    }
}
