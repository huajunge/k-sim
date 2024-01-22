package com.just.ksim.urbancomputing.utils;

/**
 * @author : hehuajun3
 * @description : 数值的工具类
 * @date : Created in 2019-05-26 12:03
 * @modified by :
 **/
public class NumberUtil {
    /**
     * 最大的字节长度
     **/
    private static int MAX_INT_BYTE_SIZE = Integer.SIZE / 8;

    /**
     * 最大的字节长度
     **/
    private static int MAX_LONG_BYTE_SIZE = Long.SIZE / 8;

    /**
     * 将int转换为byte
     *
     * @param value 数值
     * @param size  byte数组大小
     * @return : byte[]
     **/
    public static byte[] longToBytes(long value, int size) {
        byte[] targets = new byte[size];
        for (int i = 0; i < size; i++) {
            targets[i] = (byte) (value >> (8 * i) & 0xFF);
        }
        return targets;
    }

    /**
     * 将int转换为byte
     *
     * @param value      数值
     * @param size       byte数组大小
     * @param bytes      原始byte数组
     * @param startIndex 开始copy位置
     **/
    public static void copyLongToBytes(long value, int size, final byte[] bytes, int startIndex) {
        for (int i = 0; i < size; i++) {
            bytes[startIndex + i] = (byte) (value >> (8 * i) & 0xFF);
        }
    }

    /**
     * 将bytes转换为int
     *
     * @param value 数值
     * @param size  byte数组大小
     * @return : int
     **/
    public static long bytesToLong(byte[] value, int size) {
        return bytesToNumber(value, size, MAX_LONG_BYTE_SIZE);
    }

    /**
     * 将bytes转换为long
     * [startIndex,endIndex)
     *
     * @param value      数值
     * @param startIndex 开始位置
     * @param endIndex   结束位置
     * @return : int
     **/
    public static long bytesToLong(byte[] value, int startIndex, int endIndex) {
        return bytesToNumber(value, startIndex, endIndex, MAX_LONG_BYTE_SIZE);
    }

    /**
     * 将bytes转换为int
     *
     * @param value 数值
     * @param size  byte数组大小
     * @return : int
     **/
    public static long bytesToUnsignedLong(byte[] value, int size) {
        return bytesToUnsignedNumber(value, size);
    }

    /**
     * 将bytes转换为int
     *
     * @param value      数值
     * @param startIndex 开始位置
     * @param endIndex   结束位置
     * @return : int
     **/
    public static long bytesToUnsignedLong(byte[] value, int startIndex, int endIndex) {
        return bytesToUnsignedNumber(value, startIndex, endIndex);
    }

    /**
     * 将bytes转换为int
     *
     * @param value 数值
     * @param size  byte数组大小
     * @return : int
     **/
    public static int bytesToInt(byte[] value, int size) {
        return (int) bytesToNumber(value, size, MAX_INT_BYTE_SIZE);
    }

    /**
     * 将bytes转换为int
     * [startIndex,endIndex)
     *
     * @param value      数值
     * @param startIndex 开始位置
     * @param endIndex   结束位置
     * @return : int
     **/
    public static int bytesToInt(byte[] value, int startIndex, int endIndex) {
        return (int) bytesToNumber(value, startIndex, endIndex, MAX_INT_BYTE_SIZE);
    }

    /**
     * 将bytes转换为int
     *
     * @param value 数值
     * @param size  byte数组大小
     * @return : int
     **/
    public static int bytesToUnsignedInt(byte[] value, int size) {
        return (int) bytesToUnsignedNumber(value, size);
    }

    /**
     * 将bytes转换为int
     *
     * @param value      数值
     * @param startIndex 开始位置
     * @param endIndex   结束位置
     * @return : int
     **/
    public static int bytesToUnsignedInt(byte[] value, int startIndex, int endIndex) {
        return (int) bytesToUnsignedNumber(value, startIndex, endIndex);
    }

    /**
     * 将bytes转换为整数]
     *
     * @param value           数值
     * @param size            byte数组大小
     * @param maxLongByteSize 最大字节数
     * @return : long
     **/
    public static long bytesToNumber(byte[] value, int size, int maxLongByteSize) {
        long result = bytesToUnsignedNumber(value, size);
        if (value[size - 1] < 0 && size < maxLongByteSize) {
            result -= Math.pow(2, 8 * size);
        }
        return result;
    }

    /**
     * 将bytes转换为整数
     *
     * @param value           数值
     * @param startIndex      开始位置
     * @param endIndex        结束位置
     * @param maxLongByteSize 最大字节数
     * @return : long
     **/
    public static long bytesToNumber(byte[] value, int startIndex, int endIndex, int maxLongByteSize) {
        long result = bytesToUnsignedNumber(value, startIndex, endIndex);
        int lastPosition = endIndex - 1;
        if (lastPosition >= value.length) {
            lastPosition = value.length - 1;
        }
        if (value[lastPosition] < 0 && (endIndex - startIndex) < maxLongByteSize) {
            result -= Math.pow(2, 8 * (endIndex - startIndex));
        }
        return result;
    }

    /**
     * 将bytes转换为整数
     *
     * @param value 数值
     * @param size  byte数组大小
     * @return : long
     **/
    public static long bytesToUnsignedNumber(byte[] value, int size) {
        if (null == value) {
            return 0;
        }

        long result = 0;
        for (int i = 0; i < size; i++) {
            int b = value[i] & 0xFF;
            result |= b << (8 * i);
        }

        return result;
    }

    /**
     * 将bytes转换为整数
     *
     * @param value      数值
     * @param startIndex 开始位置
     * @param endIndex   结束位置
     * @return : long
     **/
    public static long bytesToUnsignedNumber(byte[] value, int startIndex, int endIndex) {
        if (null == value) {
            return 0;
        }
        long result = 0;
        for (int i = startIndex; i < endIndex && i < value.length; i++) {
            long b = value[i] & 0xFF;
            result |= b << (8 * (i - startIndex));
        }

        return result;
    }

    /**
     * 返回最小值
     *
     * @param values 值列表
     * @return : double
     **/
    public static double min(double... values) {
        double min = Double.MAX_VALUE;
        for (double value : values) {
            if (min > value) {
                min = value;
            }
        }
        return min;
    }

    /**
     * 返回最大值
     *
     * @param values 值列表
     * @return : double
     **/
    public static double max(double... values) {
        double max = Double.MIN_VALUE;
        for (double value : values) {
            if (max < value) {
                max = value;
            }
        }
        return max;
    }
}
