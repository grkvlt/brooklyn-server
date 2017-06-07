/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.core.mgmt.internal;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.SubscriptionManager;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

/**
 * A {@link SubscriptionManager} that uses ActiveMQ to handle messages and
 * keeps track of subscribers locally.
 */
public class ActiveMQSubscriptionManager extends AbstractSubscriptionManager {

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSubscriptionManager.class);

    protected final ManagementContext mgmt;
    protected final Connection connection;
    protected final Session session;
    protected final MessageProducer publisher;

    protected final Map<String, Subscription<?>> allSubscriptions = Maps.newConcurrentMap();
    protected final SetMultimap<Object, Subscription<?>> subscriptionsBySubscriber = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    protected final SetMultimap<Object, Subscription<?>> subscriptionsByToken = Multimaps.synchronizedSetMultimap(HashMultimap.create());

    public ActiveMQSubscriptionManager(ManagementContext mgmt) {
        this.mgmt = mgmt;

        String activeMQUser = System.getProperty("org.apache.brooklyn.activemq.user");
        String activeMQPassword = System.getProperty("org.apache.brooklyn.activemq.password");
        String activeMQURL = System.getProperty("org.apache.brooklyn.activemq.url");
        if (Strings.isBlank(activeMQUser) || Strings.isBlank(activeMQPassword) || Strings.isBlank(activeMQURL)) {
            throw new IllegalStateException("ActiveMQ connection details not set for subscription manager");
        }
        try {
            this.connection = connect(activeMQUser, activeMQPassword, activeMQURL);
            this.session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            this.publisher = session.createProducer(session.createTemporaryTopic());
        } catch (JMSException jmse) {
            throw Exceptions.propagate(jmse);
        }
    }

    private Connection connect(String user, String password, String url) throws JMSException {
        try {
            ActiveMQConnection connection = ActiveMQConnection.makeConnection(user, password, url);
            connection.setClientID(Identifiers.getBase64IdFromValue(System.identityHashCode(this), 5));
            return connection;
        } catch (URISyntaxException e) {
            throw Exceptions.propagate(e);
        }
    }

    public long getNumSubscriptions() {
        return allSubscriptions.size();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized <T> SubscriptionHandle subscribe(Map<String, Object> flags, final Subscription<T> s) {
        Entity producer = s.producer;
        Sensor<T> sensor= s.sensor;
        s.subscriber = getSubscriber(flags, s);
        s.eventFilter = (Predicate<SensorEvent<T>>) flags.remove("eventFilter");
        boolean notifyOfInitialValue = Boolean.TRUE.equals(flags.remove("notifyOfInitialValue"));
        Object token = makeEntitySensorToken(producer, sensor);

        LOG.debug("Creating subscription {} for {} on {} {} in {}", new Object[] { s.id, s.subscriber, producer, sensor, this });
        allSubscriptions.put(s.id, s);
        subscriptionsByToken.put(token, s);
        if (s.subscriber != null) {
            subscriptionsBySubscriber.put(s.subscriber, s);
        }

        try {
            String topicName = Objects.toString(token);
            Topic topic = session.createTopic(topicName);
            MessageConsumer consumer = session.createConsumer(topic);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        MapMessage map = (MapMessage) message;
                        String entityId = map.getString("entity");
                        String sensorName = map.getString("sensor");
                        String type = map.getString("type");
                        T value = (T) map.getString("value");
                        long timestamp = map.getJMSTimestamp();
                        Sensor<T> sensor = Sensors.newSensor((Class<T>) Class.forName(type), sensorName);
                        Entity producer = mgmt.getEntityManager().getEntity(entityId);
                        SensorEvent<T> event = new BasicSensorEvent<T>(sensor, producer, value, timestamp);
                        s.listener.onEvent(event);
                        message.acknowledge();
                    } catch (ClassNotFoundException cnfe) {
                        throw Exceptions.propagate(cnfe);
                    } catch (JMSException jmse) {
                        throw Exceptions.propagate(jmse);
                    }
                }
            });
            s.flags.put("consumer", consumer);
        } catch (JMSException jmse) {
            throw Exceptions.propagate(jmse);
        }

        if (notifyOfInitialValue) {
            if (producer == null) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with wildcard producer: "+s);
            } else if (sensor == null) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with wildcard sensor: "+s);
            } else if (!(sensor instanceof AttributeSensor)) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with non-attribute sensor: "+s);
            } else {
                LOG.trace("sending initial value of {} -> {} to {}", new Object[] { s.producer, s.sensor, s });
                T val = (T) s.producer.getAttribute((AttributeSensor<?>) s.sensor);
                publish(new BasicSensorEvent<T>(s.sensor, s.producer, val));
            }
        }

        return s;
    }

    @Override
    public Set<SubscriptionHandle> getSubscriptionsForSubscriber(Object subscriber) {
        return ImmutableSet.copyOf(subscriptionsBySubscriber.get(subscriber));
    }

    @Override
    public synchronized Set<SubscriptionHandle> getSubscriptionsForEntitySensor(Entity source, Sensor<?> sensor) {
        Set<SubscriptionHandle> subscriptions = ImmutableSet.<SubscriptionHandle>builder()
                .addAll(subscriptionsByToken.get(makeEntitySensorToken(source, sensor)))
                .addAll(subscriptionsByToken.get(makeEntitySensorToken(null, sensor)))
                .addAll(subscriptionsByToken.get(makeEntitySensorToken(source, null)))
                .addAll(subscriptionsByToken.get(makeEntitySensorToken(null, null)))
                .build();
        return subscriptions;
    }

    /**
     * Unsubscribe the given subscription id.
     *
     * @see #subscribe(Map, Entity, Sensor, SensorEventListener)
     */
    @Override
    @SuppressWarnings("rawtypes")
    public synchronized boolean unsubscribe(SubscriptionHandle sh) {
        if (!(sh instanceof Subscription)) throw new IllegalArgumentException("Only subscription handles of type Subscription supported: sh="+sh+"; type="+(sh != null ? sh.getClass().getCanonicalName() : null));
        Subscription s = (Subscription) sh;
        Object token = makeEntitySensorToken(s.producer, s.sensor);

        Subscription removed = allSubscriptions.remove(s.id);
        subscriptionsByToken.remove(token, s);
        if (s.subscriber != null) {
            subscriptionsBySubscriber.remove(s.subscriber, s);
        }

        if (s.flags.containsKey("consumer")) {
            MessageConsumer consumer = (MessageConsumer) s.flags.get("consumer");
            try {
                consumer.close();
            } catch (JMSException jmse) {
                Exceptions.propagate(jmse);
            }
        }

        return removed != null;
    }

    @Override
    public <T> void publish(final SensorEvent<T> event) {
        try {
            Message message = session.createMapMessage();
            message.setStringProperty("entity", event.getSource().getId());
            message.setStringProperty("sensor", event.getSensor().getName());
            message.setStringProperty("type", event.getSensor().getTypeName());
            message.setObjectProperty("value", event.getValue());
            message.setJMSTimestamp(event.getTimestamp());

            String name = Objects.toString(makeEntitySensorToken(event.getSource(), event.getSensor()));
            Topic topic = session.createTopic(name);
            publisher.send(topic, message);
        } catch (JMSException jmse) {
            throw Exceptions.propagate(jmse);
        }
    }

    @Override
    public String toString() {
        return "ActiveMQSubscriptionContext("+Identifiers.getBase64IdFromValue(System.identityHashCode(this), 5)+")";
    }
}
