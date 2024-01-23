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
package org.gecko.util.pushstream.policy;

import java.io.IOException;

/**
 * 
 * @author mark
 * @since 27.01.2019
 */
public interface GBQueuePolicyMBean {
	
	int getCurrentBufferFillGrade() throws IOException;
	int getBreakThreshold() throws IOException;
	long getWaitTime() throws IOException;
	long getCurrentWaitTime() throws IOException;

}
