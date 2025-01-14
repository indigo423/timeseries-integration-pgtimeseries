/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2013-2014 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2014 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.timeseries.impl.pgtimeseries.util.rrd.v3;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.opennms.timeseries.impl.pgtimeseries.util.rrd.AbstractDS;

/**
 * The Class DS (Data Source).
 * <ul>
 * <li><b>ds.decl.normal:</b> minimal_heartbeat, min, max</li>
 * <li><b>ds.decl.cdef:</b> cdef</li>
 * <li><b>ds.decl:</b> name, type, (ds.decl.normal | ds.decl.cdef), last_ds, value, unknown_sec</li>
 * </ul>
 * 
 * @author Alejandro Galue <agalue@opennms.org>
 */
@XmlRootElement(name="ds")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class DS extends AbstractDS {

    /** The type of the datasource. */
    private DSType type;

    /** The CDEF (Computed Datasource). */
    private String cdef;

    /**
     * Gets the type.
     *
     * @return the type
     */
    @XmlElement(required=true)
    @XmlJavaTypeAdapter(DSAdapter.class)
    public DSType getType() {
        return type;
    }

    /**
     * Sets the type.
     *
     * @param type the new type
     */
    public void setType(DSType type) {
        this.type = type;
    }

    /**
     * Sets the type.
     *
     * @param type the new type
     */
    public void setType(String type) {
        this.type = DSType.fromValue(type);
    }

    /**
     * Gets the CDEF.
     *
     * @return the CDEF
     */
    @XmlElement
    public String getCdef() {
        return cdef;
    }

    /**
     * Sets the CDEF.
     * <p>The XML may contain spaces that must be removed.</p>
     *
     * @param cdef the new CDEF
     */
    public void setCdef(String cdef) {
        this.cdef = cdef == null ? null : cdef.trim();
    }

    /**
     * Format equals.
     *
     * @param ds the DS object
     * @return true, if successful
     */
    public boolean formatEquals(DS ds) {
        if (this.type != null) {
            if (ds.type == null) return false;
            else if (!(this.type.equals(ds.type))) 
                return false;
        }
        else if (ds.type != null)
            return false;

        if (this.cdef != null) {
            if (ds.cdef == null) return false;
            else if (!(this.cdef.equals(ds.cdef))) 
                return false;
        }
        else if (ds.cdef != null)
            return false;

        return super.formatEquals(ds);
    }

    /* (non-Javadoc)
     * @see org.opennms.timeseries.impl.pgtimeseries.util.rrd.AbstractDS#isCounter()
     */
    @Override
    public boolean isCounter() {
        return !getType().equals(DSType.GAUGE);
    }
}
