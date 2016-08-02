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
package org.mycat.netty.mysql.respo;

import com.openddal.result.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.Types;

/**
 * Show engines: keeping some tools happy, such as MySQL workbench.
 * 
 * @author <a href="mailto:pzp@maihesoft.com">little-pan</a>
 * @since 2016-07-13
 */
public final class ShowEngines {
	
	// Simply use SimpleResultSet
	private final static ResultSet Engines(){
		return (new SimpleResultSet(){
			// - init
			{
				// add-columns
				addColumn("Engine",       Types.VARCHAR, Integer.MAX_VALUE, 0);
				addColumn("Support",      Types.VARCHAR, Integer.MAX_VALUE, 0);
				addColumn("Comment",      Types.VARCHAR, Integer.MAX_VALUE, 0);
				addColumn("Transactions", Types.VARCHAR, Integer.MAX_VALUE, 0);
				addColumn("XA",           Types.VARCHAR, Integer.MAX_VALUE, 0);
				addColumn("Savepoints",   Types.VARCHAR, Integer.MAX_VALUE, 0);
				// add-rows
				// @author little-pan
				// @since 2016-07-17
				addRow(
						"OpenDDAL",
						"Yes", 
						"JDBC-shars, database middleware, distributed SQL engine", 
						"Yes", 
						"Yes", 
						"Yes"
				);
			}
		});
	}
	
	private ShowEngines(){}
	
    public final static ResultSet getResultSet() {
    	return Engines();
    }
    
}
