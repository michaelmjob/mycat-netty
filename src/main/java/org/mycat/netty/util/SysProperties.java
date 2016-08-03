package org.mycat.netty.util;

/**
 * Created by snow_young on 16/8/3.
 */
public class SysProperties {

    public static final String FILE_ENCODING = "Cp1252";

    public static final String FILE_SEPARATOR =  "/";

    public static final String JAVA_SPECIFICATION_VERSION =   "1.4";

    public static final String LINE_SEPARATOR =  "\n";

    public static final String USER_HOME =  "";

    public static final String ALLOWED_CLASSES =  "*";

    public static final boolean CHECK =  true;

    public static final boolean CHECK2 =  false;

    public static final String CLIENT_TRACE_DIRECTORY =  "trace.db/";

    public static final int COLLATOR_CACHE_SIZE =  32000;

    public static final boolean JAVA_SYSTEM_COMPILER =  true;

    public static final int LOB_FILES_PER_DIRECTORY =  256;

    public static final int LOB_CLIENT_MAX_SIZE_MEMORY =  1024 * 1024;

    public static final int MAX_FILE_RETRY =   16;

    public static final int MAX_MEMORY_ROWS =  40000;

    public static final long MAX_TRACE_DATA_LENGTH =  65535;

    public static final boolean OBJECT_CACHE =  true;

    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = 
            4096;

    public static final int OBJECT_CACHE_SIZE =  1024;

    public static final boolean OLD_STYLE_OUTER_JOIN =  false;

    public static final String PG_DEFAULT_CLIENT_ENCODING =  "UTF-8";

    public static final String PREFIX_TEMP_FILE =  "ddal.temp";

    public static final int SERVER_RESULT_SET_FETCH_SIZE =  100;

    public static final int SOCKET_CONNECT_RETRY =  16;

    public static final int SOCKET_CONNECT_TIMEOUT =  2000;

    public static final boolean SORT_BINARY_UNSIGNED =  true;

    public static final boolean SORT_NULLS_HIGH =  false;

    public static final String SYNC_METHOD =  "sync";

    public static final boolean TRACE_IO =  false;

    public static final boolean USE_THREAD_CONTEXT_CLASS_LOADER = 
            false;

    public static final String JAVA_OBJECT_SERIALIZER =  null;

    public static final int THREAD_QUEUE_SIZE =  20480;

    public static final int THREAD_POOL_SIZE_CORE =  Runtime.getRuntime().availableProcessors() * 2;

    public static final int THREAD_POOL_SIZE_MAX =  Runtime.getRuntime().availableProcessors() * 20;

    public static boolean serializeJavaObject =  true;

    public static final String ENGINE_CONFIG_LOCATION =  "ddal-engine.xml";

    public static final String SERVERUSER_CONFIG_LOCATION =  "users.properties";

    private SysProperties(){
        // utility class
    }
}
