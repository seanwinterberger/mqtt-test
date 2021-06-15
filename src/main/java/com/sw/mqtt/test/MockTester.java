package com.sw.mqtt.test;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.dns.AddressResolverOptions;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.impl.MqttClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class MockTester {

    private static final int NUMBER_OF_MOCKS = 250;

    protected static final Logger logger = LoggerFactory.getLogger(MockTester.class);
    protected static final String TOPIC_DEVICE_CONSUME_FROM_FORMAT = "test/command/%s";
    protected static final String TOPIC_DEVICES_PUBLISH_TO_FORMAT = "test/devicestuff/%s";

    private AtomicLong deviceReceivedCounter = new AtomicLong();
    private AtomicLong consumerRecievedCounter = new AtomicLong();
    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    private Vertx vertx;
    private String mqttHost = "localhost";
    private int mqttPort = 1883;

    public static void main(String[] args) {
        new MockTester().launchEm();
    }

    public MockTester() {
        vertx = Vertx.vertx(new VertxOptions()
                .setWorkerPoolSize(10)
                .setAddressResolverOptions(new AddressResolverOptions()
                        .setNdots(1)
                        .setMaxQueries(5)
                )
                .setPreferNativeTransport(true)
                .setMaxWorkerExecuteTime(2 * 60L * 1000 * 1000000) // 2min
        );
    }

    public void launchEm() {
        logger.warn("Launching the mock devices");
        setupMockConsumer();
        setupMockDevices();
    }

    private void setupMockConsumer() {
        MqttClientOptions mqttClientOptions = getMqttOptsForMockConsumer();

        executorService.submit(() -> {
            MqttClient consumerClient = new MqttClientImpl(vertx, mqttClientOptions);

            consumerClient
                    .exceptionHandler(event -> {
                        logger.error("Something failed during setup of consumer mock - {}", event.getMessage(), event.getCause());
                    })
                    .publishHandler(event -> {
                        //Publish the message back to the client
                        logger.warn("Received {} publishing back - count is {} expect to finish at {}", event.payload(), consumerRecievedCounter.incrementAndGet(), NUMBER_OF_MOCKS);
                        consumerClient.publish(String.format(TOPIC_DEVICE_CONSUME_FROM_FORMAT, event.payload().toString()), event.payload(), MqttQoS.AT_LEAST_ONCE, false, false);
                    })
                    .connect(mqttPort, mqttHost, result -> {
                        logger.warn("Connected consumer mock - ", mqttClientOptions.getClientId());
                        //Sub for all messages from devices
                        consumerClient.subscribe(String.format(TOPIC_DEVICES_PUBLISH_TO_FORMAT, "#"), 1);
                    });
        });
    }

    private MqttClientOptions getMqttOptsForMockConsumer() {
        MqttClientOptions mqttClientOptions = new MqttClientOptions();
        mqttClientOptions.setClientId("consumermock")
                .setWillTopic("test/mock/consumer")
                .setMaxInflightQueue(100)
                .setCleanSession(false)
                .setWillMessage("")
                .setWillFlag(true)
                .setWillRetain(false)
                .setMaxMessageSize(126000)
                .setReconnectAttempts(-1)
                .setLogActivity(false);
        return mqttClientOptions;
    }

    public void setupMockDevices() {

        int randomMockGroup = new Random().nextInt(10000);
        for (int i = 0; i < NUMBER_OF_MOCKS; ++i) {
            int deviceId = i;
            executorService.submit(() -> {
                String mockClientId = String.format("test%d-%d", randomMockGroup, deviceId);
                MqttClientOptions mockDeviceOpts = getMqttOptsForMockDevice(mockClientId);

                MqttClient mqttClient = new MqttClientImpl(vertx, mockDeviceOpts);
                mqttClient.connect(mqttPort, mqttHost, result -> {
                    logger.warn("Connected Device Mock - ", mockClientId);
                    //Sub for commands for this mock
                    mqttClient.subscribe(String.format(TOPIC_DEVICE_CONSUME_FROM_FORMAT, mockDeviceOpts.getClientId()), 1, subResult -> {
                        if (subResult.succeeded()) {
                            //Publish to TOPIC_DEVICES_PUBLISH_TO with the clientID, nothing special
                            mqttClient.publish(String.format(TOPIC_DEVICES_PUBLISH_TO_FORMAT, mockDeviceOpts.getClientId()), Buffer.buffer(mockDeviceOpts.getClientId()), MqttQoS.AT_LEAST_ONCE, false, false);
                        }
                    });
                }).publishHandler(event -> {
                    logger.warn("Received response for {} - count is {} expect to finish at {}", mockDeviceOpts.getClientId(), deviceReceivedCounter.incrementAndGet(), NUMBER_OF_MOCKS);
                });

            });
        }
    }

    private MqttClientOptions getMqttOptsForMockDevice(String mockClientId) {
        MqttClientOptions mockDevice = new MqttClientOptions();
        mockDevice.setClientId(mockClientId)
                .setMaxInflightQueue(100)
                .setWillTopic("test/will/" + mockClientId)
                .setWillMessage("")
                .setWillFlag(true)
                .setWillRetain(false)
                .setCleanSession(false)
                .setMaxMessageSize(126000)
                .setReconnectAttempts(-1)
                .setLogActivity(false);
        return mockDevice;
    }

}
