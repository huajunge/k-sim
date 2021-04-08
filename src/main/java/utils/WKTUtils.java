/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package utils;


import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class WKTUtils {
    public static Geometry read(String WKTgeometry) {
        WKTReader reader = new WKTReader();
        try {
            return reader.read(WKTgeometry);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}
