/**
 * Created by vinay on 6/10/17.
 */
package storm.starter;
public class Kafka_Constants {
    public static final String BOOTSTRAP_SERVERS= "localhost:9092";
    public static final String ACKS= "all";
    public static final int RETRIES= 0;
    public static final int BATCH_SIZE= 16384;
    public static final int LINGER= 1;
    public static final int BUFFER_MEMORY= 33554432;
    public static final String KEY_SERIALIZER= "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
}
