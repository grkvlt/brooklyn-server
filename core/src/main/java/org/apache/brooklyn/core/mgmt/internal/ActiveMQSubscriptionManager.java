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

import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.join;

import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnection;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.SubscriptionManager;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.SingleThreadedScheduler;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;

/**
 * A {@link SubscriptionManager} that stores subscription details locally.
 */
public class ActiveMQSubscriptionManager extends AbstractSubscriptionManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQSubscriptionManager.class);

    protected final ManagementContext mgmt;
    protected final ExecutionManager em;
    protected final Connection connection;
    protected final Session session;
    protected final Topic topic;
    protected final MessageProducer producer;
    
    private final String tostring = "ActiveMQSubscriptionContext("+Identifiers.getBase64IdFromValue(System.identityHashCode(this), 5)+")";

    private final AtomicLong totalEventsPublishedCount = new AtomicLong();
    private final AtomicLong totalEventsDeliveredCount = new AtomicLong();
    
    @SuppressWarnings("rawtypes")
    protected final ConcurrentMap<String, Subscription> allSubscriptions = new ConcurrentHashMap<String, Subscription>();
    @SuppressWarnings("rawtypes")
    protected final ConcurrentMap<Object, Set<Subscription>> subscriptionsBySubscriber = new ConcurrentHashMap<Object, Set<Subscription>>();
    @SuppressWarnings("rawtypes")
    protected final ConcurrentMap<Object, Set<Subscription>> subscriptionsByToken = new ConcurrentHashMap<Object, Set<Subscription>>();
    
    public ActiveMQSubscriptionManager(ExecutionManager em, ManagementContext mgmt) {
        this.em = em;
        this.mgmt = mgmt;
        String activeMQUser = System.getProperty("io.brooklyn.activemq.user");
        String activeMQPassword = System.getProperty("io.brooklyn.activemq.password");
        String activeMQURL = System.getProperty("io.brooklyn.activemq.url");
        if (Strings.isBlank(activeMQUser) || Strings.isBlank(activeMQPassword) || Strings.isBlank(activeMQURL)) {
            throw new IllegalStateException("ActiveMQ connection details not set for subscription manager");
        }
        try {
            this.connection = connect(activeMQUser, activeMQPassword, activeMQURL);
            this.session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            this.topic = session.createTopic("sensors");
            this.producer = session.createProducer(topic);
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

    /** The total number of sensor change events generated (irrespective of number subscribers, see {@link #getTotalEventsDelivered()}) */
    public long getTotalEventsPublished() {
        return totalEventsPublishedCount.get();
    }
    
    /** The total number of sensor change events submitted for delivery, counting multiple deliveries for multipe subscribers (see {@link #getTotalEventsPublished()}),
     * but excluding initial notifications, and incremented when submitted ie prior to delivery */
    public long getTotalEventsDelivered() {
        return totalEventsDeliveredCount.get();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    protected synchronized <T> SubscriptionHandle subscribe(Map<String, Object> flags, final Subscription<T> s) {
        Entity producer = s.producer;
        Sensor<T> sensor= s.sensor;
        s.subscriber = getSubscriber(flags, s);
        s.eventFilter = (Predicate<SensorEvent<T>>) flags.remove("eventFilter");
        boolean notifyOfInitialValue = Boolean.TRUE.equals(flags.remove("notifyOfInitialValue"));

        if (LOG.isDebugEnabled()) LOG.debug("Creating subscription {} for {} on {} {} in {}", new Object[] {s.id, s.subscriber, producer, sensor, this});

        try {
            String topicName = makeEntitySensorToken(producer, sensor).toString();
            Topic topic = session.createTopic(topicName);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, topicName);
            subscriber.setMessageListener(new MessageListener() {
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
        } catch (JMSException jmse) {
            throw Exceptions.propagate(jmse);
        }

        if (notifyOfInitialValue) {
            if (producer == null) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with wildcard producer: "+s);
            } else if (sensor == null) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with wilcard sensor: "+s);
            } else if (!(sensor instanceof AttributeSensor)) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with non-attribute sensor: "+s);
            } else {
                if (LOG.isTraceEnabled()) LOG.trace("sending initial value of {} -> {} to {}", new Object[] {s.producer, s.sensor, s});
                T val = (T) s.producer.getAttribute((AttributeSensor<?>) s.sensor);
                publish(new BasicSensorEvent<T>(s.sensor, s.producer, val));
            }
        }
        
        return s;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<SubscriptionHandle> getSubscriptionsForSubscriber(Object subscriber) {
        return (Set<SubscriptionHandle>) ((Set<?>) elvis(subscriptionsBySubscriber.get(subscriber), Collections.emptySet()));
    }

    @Override
    public synchronized Set<SubscriptionHandle> getSubscriptionsForEntitySensor(Entity source, Sensor<?> sensor) {
        Set<SubscriptionHandle> subscriptions = new LinkedHashSet<SubscriptionHandle>();
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(source, sensor)), Collections.emptySet()));
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(null, sensor)), Collections.emptySet()));
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(source, null)), Collections.emptySet()));
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(null, null)), Collections.emptySet()));
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
        String name = makeEntitySensorToken(s.producer, s.sensor).toString();
        try {
            session.unsubscribe(name);
        } catch (JMSException jmse) {
            Exceptions.propagate(jmse);
        }
        return true;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> void publish(final SensorEvent<T> event) {
        try {
            Message message = session.createMapMessage();
            message.setStringProperty("entity", event.getSource().getId());
            message.setStringProperty("sensor", event.getSensor().getName());
            message.setStringProperty("type", event.getSensor().getTypeName());
            message.setObjectProperty("value", event.getValue());
            message.setJMSTimestamp(event.getTimestamp());
            producer.send(message);
        } catch (JMSException jmse) {
            throw Exceptions.propagate(jmse);
        }
    }

    @Override
    public String toString() {
        return tostring;
    }

}
