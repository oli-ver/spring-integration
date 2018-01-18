/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.integration.zmq.config.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.endpoint.EventDrivenConsumer;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.integration.zmq.core.DefaultZmqClientFactory;
import org.springframework.integration.zmq.outbound.ZmqMessageHandler;
import org.springframework.integration.zmq.support.DefaultZmqMessageConverter;
import org.springframework.integration.zmq.support.ZmqMessageConverter;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Subhobrata Dey
 * @since 5.1
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class ZmqOutboundChannelAdapterParserTests {

	@Autowired @Qualifier("withConverter")
	private EventDrivenConsumer withConverterEndpoint;

	@Autowired @Qualifier("withConverter.handler")
	private MessageHandler withConverterHandler;

	@Autowired @Qualifier("withDefaultConverter.handler")
	private ZmqMessageHandler withDefaultConverterHandler;

	@Autowired
	private ZmqMessageConverter converter;

	@Autowired
	private DefaultZmqClientFactory clientFactory;

	@Test
	public void testWithConverter() {
		assertEquals("tcp://*:5559", TestUtils.getPropertyValue(withConverterHandler, "url"));
		assertEquals("serverId1", TestUtils.getPropertyValue(withConverterHandler, "clientId"));
		assertEquals("zmq-foo", TestUtils.getPropertyValue(withConverterHandler, "topic"));
		assertSame(converter, TestUtils.getPropertyValue(withConverterHandler, "converter"));
		assertSame(clientFactory, TestUtils.getPropertyValue(withConverterHandler, "clientFactory"));
	}

	@Test
	public void testWithDefaultConverter() {
		assertEquals("tcp://*:5560", TestUtils.getPropertyValue(withDefaultConverterHandler, "url"));
		assertEquals("serverId2", TestUtils.getPropertyValue(withDefaultConverterHandler, "clientId"));
		assertEquals("zmq-foo", TestUtils.getPropertyValue(withDefaultConverterHandler, "topic"));
		DefaultZmqMessageConverter defaultConverter = TestUtils.getPropertyValue(withDefaultConverterHandler,
				"converter", DefaultZmqMessageConverter.class);
		assertNotNull(defaultConverter);
		assertSame(clientFactory, TestUtils.getPropertyValue(withDefaultConverterHandler, "clientFactory"));
	}
}
