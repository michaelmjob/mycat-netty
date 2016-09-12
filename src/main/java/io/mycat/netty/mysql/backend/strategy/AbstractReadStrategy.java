package io.mycat.netty.mysql.backend.strategy;

import io.mycat.netty.mysql.backend.datasource.Host;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by snow_young on 16/9/12.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public abstract class AbstractReadStrategy implements ReadStrategy {
    protected Host[] hosts;
}
