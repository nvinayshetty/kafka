import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException {
        Logger logger=Logger.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client=createElasticSearchClient();

       // client.close();

        KafkaConsumer<String,String> kafkaConsumer=createKafkaConsumer("twitter_tweets");

        while(true) {
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record:records){
                String tweet = record.value();
                IndexRequest indexRequest=new IndexRequest("twitter").source(tweet, XContentType.JSON);
                IndexResponse response=client.index(indexRequest, RequestOptions.DEFAULT);
                String id=response.getId();
                System.out.println(id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }
    }
    public static KafkaConsumer createKafkaConsumer(String topic){
        String bootStrapServer="127.0.0.1:9092";
        String groupId="kafka-demo-elastic-search";
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        return kafkaConsumer;
    }

    public static RestHighLevelClient createElasticSearchClient(){
        String hostName="vinayas-first-sandbo-333752217.us-east-1.bonsaisearch.net";
        String userName="gbokverxqi";
        String password="wtvmghxkfh";
        final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,password));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(hostName,443,"https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider);
            }
        });
        RestHighLevelClient client=new RestHighLevelClient(restClientBuilder);
        return client;
    }
}
