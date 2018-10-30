package kafka;


public interface SourceReader {
    /**
     * 读取加工数据源方法
     */
    void read();

    /**
     * 配置加载方法
     */
    void loadConfig();

    /**
     * 关闭资源方法
     */
    void close();
}
