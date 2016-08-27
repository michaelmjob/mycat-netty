package io.mycat.netty.conf;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by snow_young on 16/8/7.
 */
@Data
public class SchemaConfig {
    private final Random random = new Random();

    private static final Logger logger = LoggerFactory.getLogger(SchemaConfig.class);

//    @Getter
    Map<String, TableConfig> tables;
//    List<String>  list;

    public SchemaConfig(){ tables = new HashMap<>();}

}
