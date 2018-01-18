/*
 * Copyright 2002-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.zmq;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.commons.logging.Log;
import org.junit.Test;

import org.mockito.internal.stubbing.answers.CallsRealMethods;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.integration.channel.NullChannel;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.integration.zmq.event.ZmqIntegrationEvent;
import org.springframework.integration.zmq.event.ZmqSubscribedEvent;
import org.springframework.integration.zmq.inbound.ZmqMessageDrivenChannelAdapter;
import org.springframework.integration.zmq.outbound.ZmqMessageHandler;
import org.springframework.integration.zmq.support.DefaultZmqMessageConverter;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.ReflectionUtils;
import org.zeromq.ZAuth;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import org.springframework.integration.zmq.core.ConsumerStopAction;
import org.springframework.integration.zmq.core.DefaultZmqClientFactory;
import org.zeromq.ZMQException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Subhobrata Dey
 *
 * @since 5.1
 *
 */
public class ZmqAdapterTests {

	@Test
	public void testZmqConnectOptions() {
		DefaultZmqClientFactory factory = new DefaultZmqClientFactory();
		factory.setCleanSession(false);
		factory.setPassword("password");
		factory.setUserName("username");
		factory.setClientType(ZMQ.PUB);
		factory.setServerURI("tcp://*:5556");
		factory.setConsumerStopAction(ConsumerStopAction.UNSUBSCRIBE_ALWAYS);

		assertEquals(false, factory.cleanSession());
		assertEquals("password", factory.getPassword());
		assertEquals("username", factory.getUserName());
		assertEquals("tcp://*:5556", factory.getServerURI());
		assertEquals(ConsumerStopAction.UNSUBSCRIBE_ALWAYS, factory.getConsumerStopAction());
	}

	@Test
	public void testOutboundOptionsApplied() throws Exception {
		DefaultZmqClientFactory factory = new DefaultZmqClientFactory();
		factory.setCleanSession(false);
		factory.setPassword("password");
		factory.setUserName("username");
		factory.setClientType(ZMQ.PUB);
		factory.setServerURI("tcp://*:5556");

		factory = spy(factory);
		final ZContext zContext = mock(ZContext.class);
		willAnswer(invocation -> zContext).given(factory).getZContext();
		final ZAuth zAuth = mock(ZAuth.class);
		willAnswer(invocation -> zAuth).given(factory).getZAuth();
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		willAnswer(invocation -> client).given(factory).getClientInstance(anyString(), any());
		final ZMQ.Poller poller = mock(ZMQ.Poller.class);
		willAnswer(invocation -> poller).given(factory).getPollerInstance(anyInt());
		willAnswer(invocation -> true).given(poller).pollout(anyInt());

		ZmqMessageHandler handler = new ZmqMessageHandler("tcp://*:5556", "bar", factory);
		handler.setTopic("zmq-foo");
		handler.setConverter(new DefaultZmqMessageConverter());

		final AtomicBoolean connectCalled = new AtomicBoolean();
		willAnswer(invocation -> {
			String serverUri = invocation.getArgument(0);
			assertEquals("tcp://*:5556", serverUri);
			connectCalled.set(true);
			return true;
		}).given(client).bind(anyString());

		final AtomicBoolean publishCalled = new AtomicBoolean();
		willAnswer(invocation -> {
			byte[] messagePayload = invocation.getArgument(0);
			assertEquals("zmq-foo Hello, world!", new String(messagePayload));
			publishCalled.set(true);
			return true;
		}).given(client).send(any(byte[].class), anyInt());

		handler.start();
		handler.publish("zmq-foo",
				"Hello, world!".getBytes(Charset.defaultCharset()),
				new GenericMessage<>("Hello, world!"));

		while (!publishCalled.get()) {
			Thread.sleep(10);
		}

		verify(client, times(1)).bind(anyString());
		assertTrue(connectCalled.get());
	}

	@Test
	public void testInboundOptionsApplied() throws Exception {
		DefaultZmqClientFactory factory = new DefaultZmqClientFactory();
		factory.setCleanSession(false);
		factory.setPassword("password");
		factory.setUserName("username");
		factory.setClientType(ZMQ.SUB);
		factory.setServerURI("tcp://localhost:5556");

		factory = spy(factory);
		final ZMQ.Context context = mock(ZMQ.Context.class);
		willAnswer(invocation -> context).given(factory).getContext();
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		willAnswer(invocation -> client).given(factory).getClientInstance(anyString(), anyString());
		final ZMQ.Poller poller = mock(ZMQ.Poller.class);
		willAnswer(invocation -> poller).given(factory).getPollerInstance(anyInt());
		willAnswer(invocation -> true).given(poller).pollin(anyInt());

		ZmqMessageDrivenChannelAdapter handler =
				new ZmqMessageDrivenChannelAdapter("tcp://localhost:5556", "bar", factory);
		handler.setTopic("zmq-foo");
		handler.setConverter(new DefaultZmqMessageConverter());
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.initialize();
		handler.setTaskScheduler(taskScheduler);
		handler.setBeanFactory(mock(BeanFactory.class));

		ApplicationEventPublisher applicationEventPublisher = mock(ApplicationEventPublisher.class);
		final BlockingQueue<ZmqIntegrationEvent> events = new LinkedBlockingQueue<>();
		willAnswer(invocation -> {
			events.add(invocation.getArgument(0));
			return null;
		}).given(applicationEventPublisher).publishEvent(any(ZmqIntegrationEvent.class));
		handler.setApplicationEventPublisher(applicationEventPublisher);

		final AtomicBoolean connectCalled = new AtomicBoolean();
		willAnswer(invocation -> {
			String serverUri = invocation.getArgument(0);
			assertEquals("tcp://localhost:5556", serverUri);
			connectCalled.set(true);
			return true;
		}).given(client).connect(anyString());

		final AtomicBoolean subscribeCalled = new AtomicBoolean();
		willAnswer(invocation -> {
			subscribeCalled.set(true);
			return "zmq-foo Hello, world!".getBytes(Charset.defaultCharset());
		}).given(client).recv(anyInt());

		handler.start();

		verify(client, times(1)).connect(anyString());
		assertTrue(connectCalled.get());

		int n = 0;
		ZmqIntegrationEvent event = events.poll(10, TimeUnit.SECONDS);
		while (!(event instanceof ZmqSubscribedEvent && n++ < 20)) {
			event = events.poll(10, TimeUnit.SECONDS);
		}
		assertThat(event, instanceOf(ZmqSubscribedEvent.class));
		assertEquals("Connected and subscribed to zmq-foo", ((ZmqSubscribedEvent) event).getMessage());

		taskScheduler.destroy();
	}

	@Test
	public void testStopActionDefault() {
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		ZmqMessageDrivenChannelAdapter adapter = buildAdapter(client, true, null);

		adapter.start();
		adapter.stop();
		verifyUnsubscribe(client);
	}

	@Test
	public void testStopActionDefaultNotClean() {
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		ZmqMessageDrivenChannelAdapter adapter = buildAdapter(client, false, null);

		adapter.start();
		adapter.stop();
		verifyNotUnsubscribe(client);
	}

	@Test
	public void testStopActionAlways() {
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		ZmqMessageDrivenChannelAdapter adapter =
				buildAdapter(client, false, ConsumerStopAction.UNSUBSCRIBE_ALWAYS);

		adapter.start();
		adapter.stop();
		verifyUnsubscribe(client);

		TaskScheduler taskScheduler = TestUtils.getPropertyValue(adapter, "taskScheduler", TaskScheduler.class);

		verify(taskScheduler, never()).schedule(any(Runnable.class), any(Date.class));
	}

	@Test
	public void testUnsubscribeNever() {
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		ZmqMessageDrivenChannelAdapter adapter =
				buildAdapter(client, false, ConsumerStopAction.UNSUBSCRIBE_NEVER);

		adapter.start();
		adapter.stop();
		verifyNotUnsubscribe(client);
	}

	@Test
	public void testReconnect() throws Exception {
		final ZMQ.Socket client = mock(ZMQ.Socket.class);
		ZmqMessageDrivenChannelAdapter adapter =
				buildAdapter(client, false, ConsumerStopAction.UNSUBSCRIBE_NEVER);
		adapter.setRecoveryInterval(10);
		Log logger = spy(TestUtils.getPropertyValue(adapter, "logger", Log.class));
		new DirectFieldAccessor(adapter).setPropertyValue("logger", logger);
		given(logger.isDebugEnabled()).willReturn(true);
		final AtomicInteger attemptingReconnectCount = new AtomicInteger();
		willAnswer(i -> {
			if (attemptingReconnectCount.getAndIncrement() == 0) {
				adapter.connectionLost(new RuntimeException("while schedule running"));
			}
			i.callRealMethod();
			return null;
		}).given(logger).debug("Attempting reconnect");
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.initialize();
		adapter.setTaskScheduler(taskScheduler);
		adapter.start();
		adapter.connectionLost(new RuntimeException("initial"));
		Thread.sleep(1000);
		assertThat(attemptingReconnectCount.get(), lessThanOrEqualTo(2));
		adapter.stop();
		taskScheduler.destroy();
	}

	@Test
	public void testSubscribeFailure() throws Exception {
		DefaultZmqClientFactory factory = new DefaultZmqClientFactory();
		factory.setCleanSession(false);
		factory.setPassword("password");
		factory.setUserName("username");
		factory.setClientType(ZMQ.SUB);

		factory = spy(factory);
		ZMQ.Socket client = mock(ZMQ.Socket.class);
		willAnswer(invocation -> client).given(factory).getClientInstance(anyString(), anyString());
		willAnswer(new CallsRealMethods()).given(client).connect(anyString());

		ZmqMessageDrivenChannelAdapter adapter = new ZmqMessageDrivenChannelAdapter("foo", "bar", factory);
		adapter.setTopic("zmq-foo");
		AtomicReference<Method> method = new AtomicReference<>();
		ReflectionUtils.doWithMethods(ZmqMessageDrivenChannelAdapter.class, m -> {
			m.setAccessible(true);
			method.set(m);

		}, m -> m.getName().equals("connectAndSubscribe"));
		assertNotNull(method.get());
		try {
			method.get().invoke(adapter);
			fail("Expected InvocationTargetException");
		}
		catch (InvocationTargetException e) {
			assertThat(e.getCause(), instanceOf(ZMQException.class));
		}
	}


	private ZmqMessageDrivenChannelAdapter buildAdapter(final ZMQ.Socket client, Boolean cleanSession,
														ConsumerStopAction consumerStopAction) throws ZMQException {
		DefaultZmqClientFactory factory = new DefaultZmqClientFactory() {
			@Override
			public ZMQ.Socket getClientInstance(String clientId, String... topic) {
				return client;
			}

			@Override
			public ZMQ.Poller getPollerInstance(int pollerType) {
				return mock(ZMQ.Poller.class);
			}
		};
		factory.setServerURI("tcp://localhost:5556");
		if (cleanSession != null) {
			factory.setCleanSession(cleanSession);
		}
		if (consumerStopAction != null) {
			factory.setConsumerStopAction(consumerStopAction);
		}
		ZmqMessageDrivenChannelAdapter adapter = new ZmqMessageDrivenChannelAdapter("client", factory);
		adapter.setTopic("zmq-foo");
		adapter.setOutputChannel(new NullChannel());
		adapter.setTaskScheduler(mock(TaskScheduler.class));
		return adapter;
	}

	private void verifyUnsubscribe(ZMQ.Socket client) {
		verify(client).connect(anyString());
		verify(client).unsubscribe(any(byte[].class));
		verify(client).disconnect(anyString());
		verify(client).close();
	}

	private void verifyNotUnsubscribe(ZMQ.Socket client) {
		verify(client).connect(anyString());
		verify(client, never()).unsubscribe(any(byte[].class));
		verify(client).disconnect(anyString());
		verify(client).close();
	}
}
