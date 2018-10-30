package kafka.readerImpl;

import cn.com.wind.td.tdf.*;
import kafka.SourceReader;
import kafka.utils.Convert;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;
import java.util.UUID;

/**
 * @Author tianfusheng
 * @Date 2018/10/22-13:34
 * @Desc wind数据源对接实现类
 */
public class WindSourceReader implements SourceReader {
    private Logger logger = Logger.getLogger(this.getClass());
    private String propsPath = "conf/wande.properties";
    private Properties props= new Properties();;
    private String topicName;
    private Producer<String, String> producer;
    TDFClient client;
    TDF_MSG msg;
    private long LastPrintTime;
    /***********************configuration***************************************/
    private final String openMarket = "SH-2-0";
    private final int openTime = 0;
    private final String subscription = "";// 代码订阅,例如"600000.sh;ag.shf;000001.sz"，需要订阅的股票(单个股票格式为原始Code+.+市场，如999999.SH)，以“;”分割，为空则订阅全市场
    private final int openTypeFlags = DATA_TYPE_FLAG.DATA_TYPE_NONE;
    //环境设置参数
    private final int HEART_BEAT_INTERVAL = 0;//Heart Beat间隔（秒数）, 若值为0则表示默认值10秒钟
    private final int MISSED_BEART_COUNT = 0;//如果没有收到心跳次数超过这个值，且没收到其他任何数据，则判断为掉线，若值0为默认次数2次
    private final int OPEN_TIME_OUT = 0;//在调TDF_Open期间，接收每一个数据包的超时时间（秒数，不是TDF_Open调用总的最大等待时间），若值为0则默认30秒

    /***********************configuration***************************************/

    /**
     * 业务实现主方法
     */
    @Override
    public void read() {
        msg = client.getMessage(100);       //getMessage是取数据函数。参数含义是：等待多少ms还没数据可读就返回。 如果调用时已有数据可读，立刻返回
        if (msg == null) {
            return;
        }
        switch (msg.getDataType()) {
            //消息分2类：系统消息（心跳，网络断开，网络连接结果，收到登陆响应，收到代码表，收到闭市，收到行情变更）
            //数据消息（行情，指数，期货，逐笔成交，逐笔委托，委托队列）。 使用getDataType获取消息类型
            //系统消息
            case TDF_MSG_ID.MSG_SYS_HEART_BEAT:
                System.out.println("收到心跳信息！");
                break;
            case TDF_MSG_ID.MSG_SYS_DISCONNECT_NETWORK:
                System.out.println("网络断开！");
                //quitFlag = true;
                break;
            case TDF_MSG_ID.MSG_SYS_CONNECT_RESULT: {
                TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);                  //getMessageData: TDF_MSG可能包含多条对应类型的消息，使用此函数取条N条
                System.out.println("网络连接结果：");
                break;
            }
            case TDF_MSG_ID.MSG_SYS_LOGIN_RESULT: {
                TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                break;
            }
            case TDF_MSG_ID.MSG_SYS_CODETABLE_RESULT: {
                System.out.println("收到代码表！");
                TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                //printCodeTable(data.getCodeTableResult().getMarket()[0]);
                break;
            }
            case TDF_MSG_ID.MSG_SYS_SINGLE_CODETABLE_RESULT: {
                TDF_SINGLE_CODE_RESULT data = TDFClient.getMessageData(msg, 0).getSingleCodeTableResult();
                String market = data.getMarket();
                System.out.println("收到单个市场代码表通知,市场=" + market + ",日期=" + data.getCodeDate() + ",代码数=" + data.getCodeCount());
                break;
            }
            case TDF_MSG_ID.MSG_SYS_MARKET_CLOSE: {
                TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                break;
            }
            case TDF_MSG_ID.MSG_SYS_QUOTATIONDATE_CHANGE: {
                TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                break;
            }
            //数据消息
            case TDF_MSG_ID.MSG_DATA_MARKET:
                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                    String str = Convert.object2String(data.getMarketData());
                    logger.info(str);
                    this.kafkaSend(str);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            case TDF_MSG_ID.MSG_DATA_INDEX:
                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                    String str = Convert.object2String(data.getIndexData());
                    logger.info(str);
                    this.kafkaSend(str);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            case TDF_MSG_ID.MSG_DATA_FUTURE:

                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            case TDF_MSG_ID.MSG_DATA_TRANSACTION:
                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            case TDF_MSG_ID.MSG_DATA_ORDERQUEUE:

                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }

                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            case TDF_MSG_ID.MSG_DATA_ORDER:

                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }

                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            //BBQ现券成交数据
            case TDF_MSG_ID.MSG_DATA_BBQTRANSACTION:
                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            //BBQ现券报价数据
            case TDF_MSG_ID.MSG_DATA_BBQBID:
                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            case TDF_MSG_ID.MSG_DATA_BROKERQUEUE:
                for (int i = 0; i < msg.getAppHead().getItemCount(); i++) {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                }
                if (System.currentTimeMillis() - LastPrintTime > 10 * 1000 && msg.getAppHead().getItemCount() > 0) {
                    System.gc();
                    LastPrintTime = System.currentTimeMillis();
                }
                break;
            default:
                break;
        }
    }

    /**
     * 配置文件加载方法
     */
    @Override
    public void loadConfig() {
        kafkaConfigLoad();
        windConfigLoad();
    }

    /**
     * 线程关闭释放资源方法
     */
    @Override
    public void close() {
        client.close();
    }

    /**
     * kafka配置文件加载方法
     */
    private void kafkaConfigLoad() {
        if (new File(propsPath).exists() && new File(propsPath).isFile()) {
            logger.error("Directly reading local OS file[{}] '" + propsPath + "'[]");
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(propsPath);
                props.load(fis);
                logger.info(props.toString());
                topicName = props.getProperty("topicName");
                if (null == topicName) {
                    logger.error("topicName is null");
                    System.exit(0);
                }
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

    /**
     * wind配置文件加载方法
     */
    private void windConfigLoad() {
        client = new TDFClient();
        this.LastPrintTime = System.currentTimeMillis();
        //构造配置参数
        TDF_OPEN_SETTING setting = new TDF_OPEN_SETTING();
        setting.setIp(props.getProperty("wind.ip"));//服务器IP
        setting.setPort(props.getProperty("wind.port")); //服务器端口
        setting.setUser(props.getProperty("wind.userName"));//登录用户名
        setting.setPwd(props.getProperty("wind.pwd"));//登录密码

        //初始订阅配置
        setting.setMarkets(openMarket);                 //市场列表，不用区分大小写，用英文字符 ; 分割；如果为空，则订阅全部市场。sh;sz;cf;shf;czc;dce;
        setting.setSubScriptions(subscription);         // 代码订阅,例如"600000.sh;ag.shf;000001.sz"，需要订阅的股票(单个股票格式为原始Code+.+市场，如999999.SH)，以“;”分割，为空则订阅全市场
        setting.setTypeFlags(openTypeFlags);            //订阅的数据类型（默认全部订阅）参见DATA_TYPE_FLAG
        setting.setTime(openTime);                        //-1表示从头传输，0表示最新行情（默认0）
        //不建议修改配置
        setting.setConnectionID(0);

        //环境设置，在Open之前调用
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_HEART_BEAT_INTERVAL, HEART_BEAT_INTERVAL);
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_MISSED_BEART_COUNT, MISSED_BEART_COUNT);
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OPEN_TIME_OUT, OPEN_TIME_OUT);
        client.setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OUT_LOG, 1);//输出日志到当前目录，否则需创建log目录后输出到log目录
        int err = client.open(setting);                //这里会做连接，登录，收取代码表操作，全部完成后返回（此函数会阻塞一段时间）
        if (err != TDF_ERR.TDF_ERR_SUCCESS) {
            System.out.printf("Can't connect to %s:%d. 程序退出！\n", props.getProperty("wind.ip"), props.getProperty("wind.port"));
            System.exit(err);
        }
    }

    /**
     * kafka发送信息实现方法
     * @param str value
     */
    private void kafkaSend(String str) {
        producer.send(new ProducerRecord<String, String>(topicName,UUID.randomUUID().toString(), str), new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e != null) {
                    //TODO 实例化发送信息，进行再次发送
                    logger.info("Topic: " + metadata.topic() + ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                    logger.error(e.getMessage(), e);
                }
            }
        });
    }
}
