package io.mycat.netty.mysql.auth;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by snow_young on 16/8/6.
 * singleton
 */
@Data
public class YamlPrivilege extends AbstractPrivilege implements Source, Privilege{
    private static final Logger logger = LoggerFactory.getLogger(YamlPrivilege.class);

    public YamlPrivilege(){
        super();
    }

    public void load() throws Exception {
        Yaml yaml = new Yaml();
        InputStream is = XmlPrivilege.class.getResourceAsStream("/user.yaml");
        yaml.loadAs(is, YamlPrivilege.class);
        logger.info("yaml privilege user: {}", this.users);
        logger.info("yaml privilege whitehost: {}", this.whitehosts);
    }

}
