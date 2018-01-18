/*
 * Copyright 2002-2017 the original author or authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.support.json.EmbeddedJsonHeadersMessageMapper;
import org.springframework.integration.support.json.JacksonJsonUtils;
import org.springframework.integration.zmq.inbound.ZmqMessageDrivenChannelAdapter;
import org.springframework.integration.zmq.outbound.ZmqMessageHandler;
import org.springframework.integration.zmq.support.DefaultZmqMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Subhobrata Dey
 *
 * @since 5.1
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class BackToBackAdapterTests {

	@Test
	public void testSingleTopic() throws Exception {
		ZmqMessageHandler adapter = new ZmqMessageHandler("tcp://*:5557", "serverId");
		adapter.setTopic("zmq-foo");
		adapter.setBeanFactory(mock(BeanFactory.class));
		adapter.setConverter(new DefaultZmqMessageConverter());
		adapter.start();
		ZmqMessageDrivenChannelAdapter inbound = new ZmqMessageDrivenChannelAdapter("tcp://localhost:5557",
				"clientId");
		inbound.setTopic("zmq-foo");
		QueueChannel outputChannel = new QueueChannel();
		inbound.setOutputChannel(outputChannel);
		inbound.setConverter(new DefaultZmqMessageConverter());
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.initialize();
		inbound.setTaskScheduler(taskScheduler);
		inbound.setBeanFactory(mock(BeanFactory.class));
		inbound.start();
		adapter.handleMessage(new GenericMessage<>("foo"));
		Thread.sleep(50000);
		Message<?> out = outputChannel.receive(50000);
		assertNotNull(out);
		adapter.stop();
		inbound.stop();
		assertEquals("zmq-foo foo", out.getPayload());
	}

	@Test
	public void testJson() throws Exception {
		ZmqMessageHandler adapter = new ZmqMessageHandler("tcp://*:5557", "serverId");
		adapter.setBeanFactory(mock(BeanFactory.class));
		EmbeddedJsonHeadersMessageMapper mapper = new EmbeddedJsonHeadersMessageMapper(
				JacksonJsonUtils.messagingAwareMapper("org.springframework"));
		DefaultZmqMessageConverter converter = new DefaultZmqMessageConverter();
		converter.setBytesMessageMapper(mapper);
		adapter.setConverter(converter);
		adapter.start();
		ZmqMessageDrivenChannelAdapter inbound = new ZmqMessageDrivenChannelAdapter("tcp://localhost:5557",
				"clientId");
		QueueChannel outputChannel = new QueueChannel();
		inbound.setOutputChannel(outputChannel);
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.initialize();
		inbound.setTaskScheduler(taskScheduler);
		inbound.setBeanFactory(mock(BeanFactory.class));
		inbound.setConverter(converter);
		inbound.start();
		adapter.handleMessage(new GenericMessage<Foo>(new Foo("bar")));
		Thread.sleep(50000);
		Message<?> out = outputChannel.receive(50000);
		assertNotNull(out);
		adapter.stop();
		inbound.stop();
		assertEquals(new Foo("bar"), out.getPayload());
	}

	@Ignore
	@Test
	public void testAddRemoveTopic() throws Exception {
		ZmqMessageHandler adapter = new ZmqMessageHandler("tcp://*:5557", "serverId");
		adapter.setTopic("zmq-foo");
		adapter.setBeanFactory(mock(BeanFactory.class));
		adapter.setConverter(new DefaultZmqMessageConverter());
		adapter.start();
		ZmqMessageDrivenChannelAdapter inbound = new ZmqMessageDrivenChannelAdapter("tcp://localhost:5557", "clientId");
		QueueChannel outputChannel = new QueueChannel();
		inbound.setConverter(new DefaultZmqMessageConverter());
		inbound.setOutputChannel(outputChannel);
		inbound.setTopic("zmq-foo");
		ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.initialize();
		inbound.setTaskScheduler(taskScheduler);
		inbound.setBeanFactory(mock(BeanFactory.class));
		inbound.start();
		adapter.handleMessage(new GenericMessage<>("foo"));
		Thread.sleep(50000);
		Message<?> out = outputChannel.receive(50000);
		assertNotNull(out);
		assertEquals("zmq-foo foo", out.getPayload());

		adapter.removeTopic("zmq-foo");
		inbound.removeTopic("zmq-foo");
		adapter.handleMessage(new GenericMessage<>("foo"));
		Thread.sleep(50000);
		out = outputChannel.receive(50000);
		assertNotNull(out);
		assertEquals("foo", out.getPayload());
		adapter.stop();
		inbound.stop();
	}

	public static class Foo {

		private String bar;

		public Foo() {
			super();
		}

		public Foo(String bar) {
			this.bar = bar;
		}

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((this.bar == null) ? 0 : this.bar.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Foo other = (Foo) obj;
			if (this.bar == null) {
				if (other.bar != null) {
					return false;
				}
			}
			else if (!this.bar.equals(other.bar)) {
				return false;
			}
			return true;
		}
	}
}
