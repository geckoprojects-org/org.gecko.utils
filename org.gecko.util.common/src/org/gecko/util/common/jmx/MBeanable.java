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
package org.gecko.util.common.jmx;

/**
 * Interface that marks the implementation to return a JMX-compatible bean.
 * The Interface must end with name 'MBean'. The implementation must have the name of the interface
 * but without 'MBean' at the end.
 * @author Mark Hoffmann
 * @since 01.02.2019
 */
public interface MBeanable {
	
	/**
	 * Returns the JMX bean.
	 * @return the JMX bean.
	 */
	Object getMBean(); 

}
