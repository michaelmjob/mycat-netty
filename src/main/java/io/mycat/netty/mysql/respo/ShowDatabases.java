/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.netty.mysql.respo;

//import org.mycat.netty.mysql.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.Types;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public final class ShowDatabases {
    public static ResultSet toMySQLResultSet(ResultSet engine) throws Exception {
//        SimpleResultSet result = new SimpleResultSet();
//        result.addColumn("DATABASE", Types.VARCHAR, Integer.MAX_VALUE, 0);
//        while (engine.next()) {
//            result.addRow(engine.getString(1));
//        }
//        return result;
        return null;
    }
}
