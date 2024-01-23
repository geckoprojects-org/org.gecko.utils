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

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.osgi.util.pushstream.PushEvent;
import org.osgi.util.pushstream.QueuePolicy;
import org.osgi.util.pushstream.QueuePolicyOption;

/**
 * {@link PushStreamContext} that gets the information out of an property map
 * @author Mark Hoffmann
 * @since 03.01.2019
 */
public class OptionSimpleEventSourceContext<T> extends AbstractOptionContext<T> implements SimplePushEventSourceContext<T> {
	
	/**
	 * Creates a new instance.
	 */
	public OptionSimpleEventSourceContext(Map<String, Object> options) {
		super(options);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getBufferSize()
	 */
	@Override
	public int getBufferSize() {
		return getBufferSize(PROP_SES_BUFFER_SIZE);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getBufferQueue()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public BlockingQueue<PushEvent<? extends T>> getBufferQueue() {
		return getValue(PROP_SES_BUFFER_QUEUE, BlockingQueue.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getQueuePolicyByName()
	 */
	@Override
	public QueuePolicy<T, BlockingQueue<PushEvent<? extends T>>> getQueuePolicyByName() {
		String name = getValue(PROP_SES_QUEUE_POLICY_BY_NAME, String.class, null);
		return PushStreamContext.getQueuePolicyByName(name);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getQueuePolicy()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public QueuePolicy<T, BlockingQueue<PushEvent<? extends T>>> getQueuePolicy() {
		return getValue(PROP_SES_QUEUE_POLICY, QueuePolicy.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getQueuePolicyOption()
	 */
	@Override
	public QueuePolicyOption getQueuePolicyOption() {
		return getValue(PROP_SES_QUEUE_POLICY_OPTION, QueuePolicyOption.class, null);
	}


}
