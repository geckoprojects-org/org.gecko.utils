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
package org.gecko.core.pool;

/**
 * 
 * @author ilenia
 * @since Dec 13, 2019
 * @deprecated use {@link org.gecko.util.pool.ConfigurablePoolConstants} instead
 */
@Deprecated
public interface ConfigurablePoolConstants {
	
	public static final String POOL_COMPONENT_NAME = "pool.componentName";
	
	public static final String POOL_NAME = "pool.name";
	
	public static final String POOL_SIZE = "pool.size";
	
	public static final String POOL_TIMEOUT = "pool.timeout";
	
	public static final String POOL_AS_SERVICE = "pool.asService";
	
	public static final String POOL_COMBINED_ID = "pool.combinedId";

}
