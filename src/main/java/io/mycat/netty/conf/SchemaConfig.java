package io.mycat.netty.conf;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by snow_young on 16/8/7.
 */
@Data
public class SchemaConfig {
    private static final Logger logger = LoggerFactory.getLogger(SchemaConfig.class);

    private List<TableConfig> tables;

    public SchemaConfig(){
        tables = new ArrayList<>();
    }
}
