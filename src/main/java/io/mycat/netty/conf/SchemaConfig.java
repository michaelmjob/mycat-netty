package io.mycat.netty.conf;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by snow_young on 16/8/7.
 */
@Data
public class SchemaConfig {
    private static final Logger logger = LoggerFactory.getLogger(SchemaConfig.class);

//    private List<TableConfig> tables;
    Map<String, TableConfig> tables;

    public SchemaConfig(){ tables = new HashMap<>();}
}
