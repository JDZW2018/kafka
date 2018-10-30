package kafka;

import java.util.Properties;

public class Task implements Runnable {

    private boolean flag = true;
    private SourceReader sourceReader;

    public Task(Class clazz) {
        this.sourceReader = SourceReaderFactory.getInstance(clazz);
        this.loadConf();
    }

    @Override
    public void run() {
        int i = 0;
        while (flag) {
            sourceReader.read();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(i);
            i++;
        }
        sourceReader.close();
        System.gc();
    }

    public void close() {
        this.flag = false;
    }

    private void loadConf(){
        sourceReader.loadConfig();
    }
}
