package kafka;

import kafka.readerImpl.FileSourceReader;
import kafka.readerImpl.WindSourceReader;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaProducer {

    public KafkaProducer() {
        System.setProperty("java.security.auth.login.config", "conf/jaas.conf");
        System.setProperty("java.security.krb5.conf", "conf/krb5.conf");
    }

    public void go() {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        //executor.execute(new Task(FileSourceReader.class));
        executor.execute(new Task(WindSourceReader.class));
    }


}
