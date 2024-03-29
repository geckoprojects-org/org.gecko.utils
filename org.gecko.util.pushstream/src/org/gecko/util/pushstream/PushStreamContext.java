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
package org.gecko.util.pushstream;

import java.util.concurrent.BlockingQueue;

import org.gecko.util.pushstream.policy.GeckoPushbackPolicyOption;
import org.gecko.util.pushstream.policy.GeckoQueuePolicyOption;
import org.gecko.util.pushstream.policy.GradualBreakingQueuePolicy;
import org.osgi.util.pushstream.PushEvent;
import org.osgi.util.pushstream.PushbackPolicy;
import org.osgi.util.pushstream.PushbackPolicyOption;
import org.osgi.util.pushstream.QueuePolicy;
import org.osgi.util.pushstream.QueuePolicyOption;

/**
 * Context interface for configuring a Pushstream
 * @author Mark Hoffmann
 * @since 03.01.2019
 */
public interface PushStreamContext<T> extends SimplePushEventSourceContext<T>{
	
	/**
	 * Returns the queue policy by name
	 * @return the queue policy
	 */
	QueuePolicy<T, BlockingQueue<PushEvent<? extends T>>> getQueuePolicyByName();
	
	/**
	 * Returns the used queue policy option
	 * @return the used queue policy option
	 */
	QueuePolicyOption getQueuePolicyOption();
	
	/**
	 * Returns the pushback policy
	 * @return the pushback policy
	 */
	PushbackPolicy<T, BlockingQueue<PushEvent<? extends T>>> getPushbackPolicy();
	
	/**
	 * Returns the pushback policy option
	 * @return the pushback policy option
	 */
	PushbackPolicyOption getPushbackPolicyOption();

	/**
	 * Returns the used pushback policy option time
	 * @return the used pushback policy option time
	 */
	Long getPushbackPolicyOptionTime();

	/**
	 * Returns the ready configured {@link PushbackPolicy}. 
	 * The name must be an enum of {@link PushbackPolicyOption} or {@link GeckoPushbackPolicyOption}. If none is found a {@link IllegalArgumentException} is thrown.
	 * This requires, that a pushback option time is configured as well. If not, this will throw a {@link IllegalArgumentException} 
	 * @return the ready configured {@link PushbackPolicy}
	 */
	<U extends BlockingQueue<PushEvent<? extends T>>> PushbackPolicy<T, U> getPushbackPolicyByName();
	
	/**
	 * Tries to find a {@link QueuePolicy} in the {@link QueuePolicyOption}, {@link GeckoQueuePolicyOption} in the described order. 
	 * If nothing was found it tries to parse the name, to determine a GRADUAL_BREAKING_POLICY.
	 * If no name was given <code>null</code> will returned.
	 * @param name the name of the policy
	 * @return the policy or <code>null</code>
	 */
	public static <T> QueuePolicy<T, BlockingQueue<PushEvent<? extends T>>> getQueuePolicyByName(String name) {
		if (name == null) {
			return null;
		}
		for (QueuePolicyOption o : QueuePolicyOption.values()) {
			if (o.name().equalsIgnoreCase(name)) {
				return o.getPolicy();
			}
		}
		for (GeckoQueuePolicyOption o : GeckoQueuePolicyOption.values()) {
			if (o.name().equalsIgnoreCase(name)) {
				return o.getPolicy();
			}
		}
		if (name.toUpperCase().startsWith(GeckoQueuePolicyOption.GRADUAL_BREAKING_POLICY.name())) {
			int buffer = 100;
			int threshold = 80;
			long time = 5;
			String paramString = name.replace(GeckoQueuePolicyOption.GRADUAL_BREAKING_POLICY.name() + "_", "");
			if (paramString != null) {
				String[] params = paramString.split("_");
				for (int i = 0; i< params.length; i++) {
					if (i == 0 || i == 1) {
						try {
							int v = Integer.parseInt(params[i]);
							switch (i) {
							case 0:
								threshold = v;
								break;
							case 1:
								buffer = v;
								break;
							}
						} catch (NumberFormatException e) {
							throw new IllegalStateException(String.format("Illegal parameter %s for gradual breaking queue policy", params[i]));
						}
					} else if (i == 2) {
						try {
							long v = Long.parseLong(params[i]);
							time = v;
						} catch (NumberFormatException e) {
							throw new IllegalStateException(String.format("Illegal parameter %s for gradual breaking queue policy", params[i]));
						}
					}
				}
			}
			return new GradualBreakingQueuePolicy<T, BlockingQueue<PushEvent<? extends T>>>(name.toUpperCase(), threshold, buffer, time);
		}
		return null;
	}

}
