package kafka.readerImpl;

import kafka.SourceReader;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.Properties;
import java.util.UUID;

public class FileSourceReader implements SourceReader {
    private static Logger logger = Logger.getLogger(FileSourceReader.class);
    private String propsPath = "conf/wande.properties";
    private String fileSourcePath = "conf/demo.txt";
    private String topicName;
    private Producer<String, String> producer;

    @Override
    public void read() {
        FileInputStream fileInputStream =
                null;
        try {
            fileInputStream = new FileInputStream(fileSourcePath);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
        String strLine;
        try {
            while ((strLine = br.readLine()) != null) {
                StringBuffer stringBuffer = new StringBuffer();
                stringBuffer.append(strLine);
                stringBuffer.append(System.currentTimeMillis());
                String key = UUID.randomUUID().toString();
                producer.send(new ProducerRecord<String, String>(topicName, key, stringBuffer.toString()),new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null) {
                            //TODO 实例化发送信息，进行再次发送
                            logger.info("Topic: " + metadata.topic() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                            logger.error(e.getMessage(), e);
                        }
                    }
                });
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                br.close();
                fileInputStream.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public void loadConfig() {
        Properties props = new Properties();
        if (new File(propsPath).exists() && new File(propsPath).isFile()) {
            logger.error("Directly reading local OS file[{}] '" + propsPath + "'[]");
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(propsPath);
                props.load(fis);
                topicName = props.getProperty("topicName");
                producer = new KafkaProducer<String, String>(props);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("Searching for '" + propsPath + "' in CLASSPATH");
            try {
                props.load(this.getClass().getResourceAsStream(propsPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {

    }
}
