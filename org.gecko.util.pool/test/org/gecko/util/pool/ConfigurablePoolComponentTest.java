/**
 * Copyright (c) 2012 - 2024 Data In Motion and others.
 * All rights reserved. 
 * 
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 * 
 * Contributors:
 *     Data In Motion - initial API and implementation
 */
package org.gecko.util.pool;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Hashtable;
import java.util.Map;

import org.gecko.util.pool.ConfigurablePoolComponent.PoolConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.osgi.framework.BundleContext;
import org.osgi.service.cm.ConfigurationException;
import org.osgi.service.component.ComponentServiceObjects;

/**
 * 
 * @author ilenia
 * @since Dec 13, 2019
 */
@ExtendWith(MockitoExtension.class)
public class ConfigurablePoolComponentTest {
	
	@Mock
	private BundleContext ctx;
	
	@Mock
	private PoolConfiguration config;
	
	@Mock
	private ComponentServiceObjects<String> serviceObj;

	
	@BeforeEach
	public void doBefore() {

	}
	
	@Test
	public void testActivation_Success()  {
		Mockito.when(config.pool_componentName()).thenReturn("test");
		ConfigurablePoolComponent<String> testPoolComponent = new ConfigurablePoolComponent<String>();
		try {
			testPoolComponent.activate(ctx, config);
		} catch (ConfigurationException e) {
			fail("Activation of Configurable Pool Component failed");
		}
		
	}
	
	@Test
	public void testActivation_NoName() throws ConfigurationException {		
		Mockito.when(config.pool_componentName()).thenReturn("");
		ConfigurablePoolComponent<String> testPoolComponent = new ConfigurablePoolComponent<String>();
		assertThrows(ConfigurationException.class, ()->testPoolComponent.activate(ctx, config));
	}

	@Test
	public void testPoolRegistration() throws ConfigurationException {
		Mockito.when(config.pool_componentName()).thenReturn("test");
		Mockito.when(serviceObj.getService()).thenReturn("test");
		ConfigurablePoolComponent<String> testPoolComponent = new ConfigurablePoolComponent<String>();
		
		Map<String, Object> properties = new Hashtable<String, Object>();
		properties.put("pool.name", "testPool");
		properties.put("pool.size", 7);
		properties.put("pool.timeout", 77);
		
		testPoolComponent.activate(ctx, config);
		testPoolComponent.registerPool(serviceObj, properties);
		
		Mockito.verify(serviceObj, Mockito.times(7)).getService();
		Mockito.verify(serviceObj, Mockito.never()).ungetService(Mockito.anyString());
		
		testPoolComponent.getPoolMap().containsKey("test-testPool");
		
		testPoolComponent.unregisterPool(serviceObj);
		
		Mockito.verify(serviceObj, Mockito.times(7)).getService();
		Mockito.verify(serviceObj, Mockito.times(7)).ungetService(Mockito.anyString());
	}
	
	
	
}
