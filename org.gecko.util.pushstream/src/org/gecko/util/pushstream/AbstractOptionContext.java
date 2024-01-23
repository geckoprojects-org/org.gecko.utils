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
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

import org.osgi.util.function.Consumer;
import org.osgi.util.function.Predicate;

/**
 * @param <T>
 * @author mark
 * @since 23.01.2024
 */
public abstract class AbstractOptionContext<T> implements SimplePushEventSourceContext<T>, PushStreamConstants {

	protected final Map<String, Object> options;

	/**
	 * Creates a new instance.
	 */
	public AbstractOptionContext(Map<String, Object> options) {
		if (nonNull(options) && !options.isEmpty()) {
			this.options = Collections.unmodifiableMap(options);
		} else {
			this.options = Collections.emptyMap();
		}
	}
	
	protected int getBufferSize(String property) {
		requireNonNull(property);
		Integer bs = getValue(property, Integer.class, Integer.valueOf(0));
		return isNull(bs) ? 0 : bs.intValue();
	}

	/**
	 * Returns the value of the key or <code>null</code>. If the provided class does not fit to the value's class an {@link IllegalStateException} will be thrown 
	 * @param key the options key
	 * @param clazz the class type of the expected value
	 * @return
	 */
	@SuppressWarnings("unchecked")
	protected <O> O getValue(String key, Class<O> clazz, O defaultValue) {
		Object o = options.getOrDefault(key, defaultValue);
		if (isNull(o)) {
			return null;
		}
		if (clazz.isAssignableFrom(o.getClass())) {
			return (O)o;
		} else {
			throw new IllegalStateException(String.format("The value of key '%s' is exepected of type '%s', but was of type '%s'", key, clazz.getName(), o.getClass().getName()));
		}
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstream.SimplePushEventSourceContext#getParallelism()
	 */
	public int getParallelism() {
		Integer p = getValue(PROP_PARALLELISM, Integer.class, Integer.valueOf(1));
		return isNull(p) ? 1 : p.intValue();
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getExecutor()
	 */
	@Override
	public ExecutorService getExecutor() {
		return getValue(PROP_EXECUTOR, ExecutorService.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getScheduler()
	 */
	@Override
	public ScheduledExecutorService getScheduler() {
		return getValue(PROP_SCHEDULED_EXECUTOR, ScheduledExecutorService.class, null);
	}
	
	
	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.PushStreamContext#getAcknowledgeErrorFunction()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public BiConsumer<Throwable, T> getAcknowledgeErrorFunction() {
		return getValue(PROP_ACK_ERROR, BiConsumer.class, null);
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getAcknowledgeFilter()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Predicate<T> getAcknowledgeFilter() {
		return  getValue(PROP_ACK_FILTER, Predicate.class, null);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getAcknowledgeFunction()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Consumer<T> getAcknowledgeFunction() {
		return getValue(PROP_ACK_CONSUMER, Consumer.class, null);
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.gecko.util.pushstreams.SimplePushEventSourceContext#getNAcknowledgeFunction()
	 */
	@SuppressWarnings("unchecked")
	@Override
	public Consumer<T> getNAcknowledgeFunction() {
		return getValue(PROP_NACK_CONSUMER, Consumer.class, null);
	}

}