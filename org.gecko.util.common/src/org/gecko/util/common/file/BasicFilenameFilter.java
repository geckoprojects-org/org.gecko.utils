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
package org.gecko.util.common.file;

import java.io.File;
import java.io.FilenameFilter;

/**
 * {@link FilenameFilter} implementation
 * @author Mark Hoffmann
 */
public class BasicFilenameFilter implements FilenameFilter {
	
	private String[] rawFilters;

	public static enum FilterType {
		EQUALS,
		STARTS_WITH,
		ENDS_WITH,
		CONTAINS
	}
	
	public BasicFilenameFilter(String[] rawFilters) {
		this.rawFilters = rawFilters == null ? new String[0] : rawFilters;
	}

	/* (non-Javadoc)
	 * @see java.io.FilenameFilter#accept(java.io.File, java.lang.String)
	 */
	@Override
	public boolean accept(File dir, String name) {
		if (rawFilters.length == 0) {
			return true;
		}
		for (String rawFilter : rawFilters) {
			if (evaluateFilterPattern(rawFilter, name)) {
				return true;
			}
		}
		return false;
	}

	private boolean evaluateFilterPattern(String rawFilter, String fileName) {
		String filterToApply = rawFilter;
		FilterType type = FilterType.EQUALS;
		if (rawFilter.startsWith("*")) {
			type = FilterType.ENDS_WITH;
			filterToApply = filterToApply.substring(1);
		}
		if (rawFilter.endsWith("*")) {
			type = type == FilterType.EQUALS ? FilterType.STARTS_WITH : FilterType.CONTAINS;
			filterToApply = filterToApply.substring(0, filterToApply.lastIndexOf("*"));
		}
		String lowercaseName = fileName.toLowerCase();
		switch (type) {
		case EQUALS:
			return lowercaseName.equalsIgnoreCase(filterToApply);
		case STARTS_WITH:
			return lowercaseName.startsWith(filterToApply);
		case ENDS_WITH:
			return lowercaseName.endsWith(filterToApply);
		case CONTAINS:
			return lowercaseName.contains(filterToApply);
		default:
			return false;
		}
	}
	
}
