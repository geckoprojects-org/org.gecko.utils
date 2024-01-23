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

import static java.util.Objects.isNull;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.gecko.util.pushstream.policy.GeckoPushbackPolicyOption;
import org.osgi.util.pushstream.PushEvent;
import org.osgi.util.pushstream.PushbackPolicy;
import org.osgi.util.pushstream.PushbackPolicyOption;
import org.osgi.util.pushstream.QueuePolicy;
import org.osgi.util.pushstream.QueuePolicyOption;

/**
 * {@link PushStreamContext} that gets the information out of an property map
 * @author Mark Hoffmann
 * @since 03.01.2019
 */
public class OptionPushStreamContext<T> extends AbstractOptionContext<T> implements PushStreamContext<T> {
	
	/**
	 * Creates a new instance.
	 */
	public OptionPushStreamContext(Map<String, Object> options) {
		super(options);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getBufferSize()
	 */
	@Override
	public int getBufferSize() {
		return getBufferSize(PROP_BUFFER_SIZE);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getBufferQueue()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public BlockingQueue<PushEvent<? extends T>> getBufferQueue() {
		return getValue(PROP_BUFFER_QUEUE, BlockingQueue.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getQueuePolicy()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public QueuePolicy<T, BlockingQueue<PushEvent<? extends T>>> getQueuePolicy() {
		return getValue(PROP_QUEUE_POLICY, QueuePolicy.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getQueuePolicyOption()
	 */
	@Override
	public QueuePolicyOption getQueuePolicyOption() {
		return getValue(PROP_QUEUE_POLICY_OPTION, QueuePolicyOption.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getPushbackPolicy()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public PushbackPolicy<T, BlockingQueue<PushEvent<? extends T>>> getPushbackPolicy() {
		return getValue(PROP_PUSHBACK_POLICY, PushbackPolicy.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getPushbackPolicyOption()
	 */
	@Override
	public PushbackPolicyOption getPushbackPolicyOption() {
		return getValue(PROP_PUSHBACK_POLICY_OPTION, PushbackPolicyOption.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getPushbackPolicyByName()
	 */
	@Override
	public <U extends BlockingQueue<PushEvent<? extends T>>> PushbackPolicy<T, U> getPushbackPolicyByName() {
		String name = getValue(PROP_PUSHBACK_POLICY_OPTION_BY_NAME, String.class, null);
		Long time = getPushbackPolicyOptionTime();
		if(isNull(time)) {
			throw new IllegalArgumentException(PROP_PUSHBACK_POLICY_OPTION_BY_NAME + " requires " + PROP_PUSHBACK_POLICY_TIME + "to be set");
		}
		PushbackPolicyOption option = null;
		for (PushbackPolicyOption pushbackPolicyOption : PushbackPolicyOption.values()) {
			if(pushbackPolicyOption.name().equals(name)) {
				option = pushbackPolicyOption;
				break;
			}
		}
		if(isNull(option)) {
			GeckoPushbackPolicyOption pushbackPolicyOption = null;
			for (GeckoPushbackPolicyOption geckoOption : GeckoPushbackPolicyOption.values()) {
				if(geckoOption.name().equals(name)) {
					pushbackPolicyOption = geckoOption;
					break;
				}
			}
			if(isNull(pushbackPolicyOption)) {
				throw new IllegalArgumentException("No PushbackPolicyOption or GeckoPushbackPolicyOption found with name " + name);
			}
			return pushbackPolicyOption.getPolicy(time);
		} else {
			return option.getPolicy(time);
		}
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getPushbackPolicyOptionTime()
	 */
	@Override
	public Long getPushbackPolicyOptionTime() {
		Long t = getValue(PROP_PUSHBACK_POLICY_TIME, Long.class, null); 
		return t;
	}
	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getQueuePolicyByName()
	 */
	@Override
	public QueuePolicy<T, BlockingQueue<PushEvent<? extends T>>> getQueuePolicyByName() {
		String name = getValue(PROP_QUEUE_POLICY_BY_NAME, String.class, null);
		return PushStreamContext.getQueuePolicyByName(name);
	}

}
