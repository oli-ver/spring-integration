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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.test.util.TestUtils;
import org.springframework.integration.zmq.core.DefaultZmqClientFactory;
import org.springframework.integration.zmq.inbound.ZmqMessageDrivenChannelAdapter;
import org.springframework.integration.zmq.support.ZmqMessageConverter;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Subhobrata Dey
 * @since 5.1
 *
 */
@RunWith(SpringRunner.class)
@DirtiesContext
public class ZmqMessageDrivenChannelAdapterParserTests {

	@Autowired
	private ZmqMessageDrivenChannelAdapter noTopicsAdapter;

	@Autowired
	private ZmqMessageDrivenChannelAdapter noTopicsAdapterDefaultCF;

	@Autowired
	private ZmqMessageDrivenChannelAdapter oneTopicAdapter;

	@Autowired
	private MessageChannel out;

	@Autowired
	private ZmqMessageConverter converter;

	@Autowired
	private DefaultZmqClientFactory clientFactory;

	@Autowired
	private MessageChannel errors;

	@Test
	public void testNoTopics() {
		assertEquals("tcp://localhost:5559", TestUtils.getPropertyValue(noTopicsAdapter, "url"));
		assertFalse(TestUtils.getPropertyValue(noTopicsAdapter, "autoStartup", Boolean.class));
		assertEquals("clientId1", TestUtils.getPropertyValue(noTopicsAdapter, "clientId"));
		assertSame(out, TestUtils.getPropertyValue(noTopicsAdapter, "outputChannel"));
		assertSame(clientFactory, TestUtils.getPropertyValue(noTopicsAdapter, "clientFactory"));
		assertEquals(5000, TestUtils.getPropertyValue(this.noTopicsAdapter, "recoveryInterval"));
	}

	@Test
	public void testNoTopicsDefaultCF() {
		assertEquals("tcp://localhost:5559", TestUtils.getPropertyValue(noTopicsAdapterDefaultCF, "url"));
		assertFalse(TestUtils.getPropertyValue(noTopicsAdapterDefaultCF, "autoStartup", Boolean.class));
		assertEquals("clientId2", TestUtils.getPropertyValue(noTopicsAdapterDefaultCF, "clientId"));
		assertEquals(out, TestUtils.getPropertyValue(noTopicsAdapterDefaultCF, "outputChannel"));
	}

	@Test
	public void testOneTopic() {
		assertEquals("tcp://localhost:5559", TestUtils.getPropertyValue(oneTopicAdapter, "url"));
		assertFalse(TestUtils.getPropertyValue(oneTopicAdapter, "autoStartup", Boolean.class));
		assertEquals("clientId4", TestUtils.getPropertyValue(oneTopicAdapter, "clientId"));
		assertSame(converter, TestUtils.getPropertyValue(oneTopicAdapter, "converter"));
		assertSame(out, TestUtils.getPropertyValue(oneTopicAdapter, "outputChannel"));
		assertSame(clientFactory, TestUtils.getPropertyValue(oneTopicAdapter, "clientFactory"));
		assertSame(errors, TestUtils.getPropertyValue(oneTopicAdapter, "errorChannel"));
	}
}
