package producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TwitterProducer {
    Logger logger=Logger.getLogger(TwitterProducer.class.getName());
    public static void main(String[] args) {
       new TwitterProducer().run();
    }

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        Client client=createTwitterClient(msgQueue);
        client.connect();
        KafkaProducer<String,String> producer=createKafkaProducer();

        while (!client.isDone()) {
            String msg=null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();

            }
            if(msg!=null){
                logger.info(msg);
            }
            logger.info("End of application");
            producer.send(new ProducerRecord("twitter_tweets",null,msg),new Callback(){

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e!=null){
                        logger.log(Level.SEVERE, e.toString());
                    }
                }
            });
        }


    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServer="127.0.0.1:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return kafkaProducer;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue ){/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("twitter", "api");
        hosebirdEndpoint.trackTerms(terms);
        String consumerKey="ELXOMPdtLCfIsVKtI8RZMQHrl";
        String consumerSecret="HQPF1a5ALSc3fPfc0tGjzHKNe5uN7OVuLvBhHsSuS1OtdSitBL";
        String token= "326654147-s3BzNbrL8xrKoYcWxqLcMHpxY2DeiALrLVbQTLET";
        String secret="oqn96DLekRLDrF4ANf7JfN6MDCmbP2csxwy0KcEHdWui9";
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
