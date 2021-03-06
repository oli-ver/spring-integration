/*
 * Copyright 2015-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.config;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.support.management.IntegrationManagement;
import org.springframework.integration.support.management.IntegrationManagement.ManagementOverrides;
import org.springframework.integration.support.management.metrics.MetricsCaptor;
import org.springframework.integration.support.management.micrometer.MicrometerMetricsCaptor;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;


/**
 * Configures beans that implement {@link IntegrationManagement}.
 * Configures counts, stats, logging for all (or selected) components.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Meherzad Lahewala
 * @author Jonathan Pearlin
 *
 * @since 4.2
 *
 */
public class IntegrationManagementConfigurer
		implements SmartInitializingSingleton, ApplicationContextAware, BeanNameAware, BeanPostProcessor {

	/**
	 * Bean name of tehe configurer.
	 */
	public static final String MANAGEMENT_CONFIGURER_NAME = "integrationManagementConfigurer";

	private ApplicationContext applicationContext;

	private String beanName;

	private boolean defaultLoggingEnabled = true;

	private volatile boolean singletonsInstantiated;

	private MetricsCaptor metricsCaptor;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Disable all logging in the normal message flow in framework components. When 'false', such logging will be
	 * skipped, regardless of logging level. When 'true', the logging is controlled as normal by the logging
	 * subsystem log level configuration.
	 * <p>
	 * Exception logging (debug or otherwise) is not affected by this setting.
	 * <p>
	 * It has been found that in high-volume messaging environments, calls to methods such as
	 * {@link Log#isDebugEnabled()} can be quite expensive and account for an inordinate amount of CPU
	 * time.
	 * <p>
	 * Set this to false to disable logging by default in all framework components that implement
	 * {@link IntegrationManagement} (channels, message handlers etc). This turns off logging such as
	 * "PreSend on channel", "Received message" etc.
	 * <p>
	 * After the context is initialized, individual components can have their setting changed by invoking
	 * {@link IntegrationManagement#setLoggingEnabled(boolean)}.
	 * @param defaultLoggingEnabled defaults to true.
	 */
	public void setDefaultLoggingEnabled(boolean defaultLoggingEnabled) {
		this.defaultLoggingEnabled = defaultLoggingEnabled;
	}

	@Override
	public void afterSingletonsInstantiated() {
		Assert.state(this.applicationContext != null, "'applicationContext' must not be null");
		Assert.state(MANAGEMENT_CONFIGURER_NAME.equals(this.beanName), getClass().getSimpleName()
				+ " bean name must be " + MANAGEMENT_CONFIGURER_NAME);
		if (ClassUtils.isPresent("io.micrometer.core.instrument.MeterRegistry",
				this.applicationContext.getClassLoader())) {
			this.metricsCaptor = MicrometerMetricsCaptor.loadCaptor(this.applicationContext);
		}
		if (this.metricsCaptor != null) {
			injectCaptor();
			registerComponentGauges();
		}
		Map<String, IntegrationManagement> managed = this.applicationContext
				.getBeansOfType(IntegrationManagement.class);
		for (Entry<String, IntegrationManagement> entry : managed.entrySet()) {
			IntegrationManagement bean = entry.getValue();
			if (!getOverrides(bean).loggingConfigured) {
				bean.setLoggingEnabled(this.defaultLoggingEnabled);
			}
			String name = entry.getKey();
		}
		this.singletonsInstantiated = true;
	}

	private void injectCaptor() {
		Map<String, IntegrationManagement> managed =
				this.applicationContext.getBeansOfType(IntegrationManagement.class);
		for (Entry<String, IntegrationManagement> entry : managed.entrySet()) {
			IntegrationManagement bean = entry.getValue();
			if (!getOverrides(bean).loggingConfigured) {
				bean.setLoggingEnabled(this.defaultLoggingEnabled);
			}
			bean.registerMetricsCaptor(this.metricsCaptor);
		}
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String name) throws BeansException {
		if (this.singletonsInstantiated) {
			if (this.metricsCaptor != null && bean instanceof IntegrationManagement) {
				((IntegrationManagement) bean).registerMetricsCaptor(this.metricsCaptor);
			}
		}
		return bean;
	}

	private void registerComponentGauges() {
		this.metricsCaptor.gaugeBuilder("spring.integration.channels", this,
				(c) -> this.applicationContext.getBeansOfType(MessageChannel.class).size())
				.description("The number of message channels")
				.build();

		this.metricsCaptor.gaugeBuilder("spring.integration.handlers", this,
				(c) -> this.applicationContext.getBeansOfType(MessageHandler.class).size())
				.description("The number of message handlers")
				.build();

		this.metricsCaptor.gaugeBuilder("spring.integration.sources", this,
				(c) -> this.applicationContext.getBeansOfType(MessageSource.class).size())
				.description("The number of message sources")
				.build();
	}

	private static ManagementOverrides getOverrides(IntegrationManagement bean) {
		return bean.getOverrides() != null ? bean.getOverrides() : new ManagementOverrides();
	}

}
