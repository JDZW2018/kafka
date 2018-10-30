package kafka.utils;

import java.lang.reflect.Field;

/**
 * @Author tianfusheng
 * @Date 2018/10/24-13:34
 * @Desc 实体类转换工具
 */
public class Convert {
    /**
     * 实体类值转字符串
     * @return
     */
    public static String object2String(Object object){
        if(object == null){
            return null;
        }
        StringBuffer sbf = new StringBuffer();
        Class clazz = object.getClass();
        Field[] fields = clazz.getDeclaredFields();
        try {
            for (Field field:fields){
                field.setAccessible(true);
                sbf.append(field.get(object));
                sbf.append(",");
            }
            sbf.deleteCharAt(sbf.length()-1);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return sbf.toString();

    }

}
