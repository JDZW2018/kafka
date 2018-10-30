package kafka;

public class SourceReaderFactory {
    public static SourceReader getInstance(Class clazz){
        try {
            SourceReader sourceReader = (SourceReader) Class.forName(clazz.getName()).newInstance();
            return sourceReader;
        } catch (InstantiationException e) {
            e.printStackTrace();
            return null;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return null;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}
