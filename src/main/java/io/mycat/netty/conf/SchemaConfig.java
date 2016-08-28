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
    private static final Logger logger = LoggerFactory.getLogger(SchemaConfig.class);

    private final Random random = new Random();

    // 暂时不需要管理这些参数
    private boolean noSharding;

    private String name;

    Map<String, TableConfig> tables;

    public SchemaConfig(){ tables = new HashMap<>();}

}
