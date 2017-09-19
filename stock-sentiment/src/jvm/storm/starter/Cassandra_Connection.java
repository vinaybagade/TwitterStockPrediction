package storm.starter;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        import org.json.simple.JSONObject;

/**
 * Created by Vinay on 19-05-2017.
 */
public class Cassandra_Connection {
    private static Cluster cluster= null;
    private static Session session = null;
    private static Session session_stockpredict = null;
    private final static String create_keyspace = "CREATE KEYSPACE IF NOT EXISTS perm_storage WITH replication "
            + "= {'class':'SimpleStrategy', 'replication_factor':1};";
    private final static String create_table = "CREATE TABLE IF NOT EXISTS STOCK_PREDICT (INSERTION_TIME TIMESTAMP PRIMARY KEY, COMPANYNAME TEXT, STOCKPRICE TEXT, PREDICTION TEXT,  SENTIMENT TEXT);";
    public static Session init(){
        if(session != null){
            session.execute(create_keyspace);
            session.execute("USE perm_storage");
            session.execute(create_table);
            return session;
        }
        return null;
    }
    private static Cluster getConnection(String hostname){
        if(cluster==null){
            cluster = Cluster.builder().addContactPoint(hostname).build();
        }
        return cluster;
    }
    public static Session getSession(String hostname){
        if(session==null){
            session = Cassandra_Connection.getConnection(hostname).connect();
        }
        return session;
    }

}
