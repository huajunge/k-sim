package utils;

import org.apache.commons.lang.NullArgumentException;

import java.util.Collection;

/**
 * @author huangyibo
 * @description : 常用工具类
 * @date 2019-03-18 12:07
 */
public class CheckUtils {

    /**
     * @param object
     * @return void
     * @description 判断某实体是否为空
     */
    public static void checkEmpty(Object... object) {
        for (Object o : object) {
            if (o == null) {
                throw new NullArgumentException("The parameter can not be null!");
            }
        }
    }

    /**
     * @param col
     * @return boolean
     * @description 判断一个集合是否为空
     */
    public static boolean isCollectionEmpty(Collection col) {
        return col == null || col.isEmpty();
    }
}
