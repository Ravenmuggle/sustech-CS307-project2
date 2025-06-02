package edu.sustech.cs307.value;

import java.nio.ByteBuffer;

public class Value {
    public Object value;
    public ValueType type;
    public static final int INT_SIZE = 64;
    public static final int FLOAT_SIZE = 64;
    public static final int CHAR_SIZE = 64;
    public static final int VARCHAR_SIZE = 64;
    public static final int DOUBLE_SIZE = 64;
    public static final int UNKNOWN_SIZE = 64;


    public Value(Object value, ValueType type) {
        this.value = value;
        this.type = type;
    }

    public Value(Long value) {
        this.value = value;
        type = ValueType.INTEGER;
    }

    public Value(Double value) {
        this.value = value;
        type = ValueType.FLOAT;
    }

    public Value(String value) {
        this.value = value;
        type = ValueType.CHAR;
    }
    public Value(){
            this.value=0L; // 默认值为 0L
            this.type=ValueType.UNKNOWN;

    }

    /**
     * 将当前值转换为字节数组。
     * 
     * @return 字节数组表示的值，根据值的类型（INTEGER、FLOAT、CHAR）进行转换。
     * @throws RuntimeException 如果值的类型不受支持。
     */
    public byte[] ToByte() {
        return switch (type) {
            case INTEGER -> {
                ByteBuffer buffer1 = ByteBuffer.allocate(64);
                buffer1.putLong((long) value);
                yield buffer1.array();
            }
            case FLOAT -> {
                ByteBuffer buffer2 = ByteBuffer.allocate(64);
                buffer2.putDouble((double) value);
                yield buffer2.array();
            }
            case CHAR -> {
                String str = (String) value;
                ByteBuffer buffer3 = ByteBuffer.allocate(64);
                buffer3.putInt(str.length());
                buffer3.put(str.getBytes());
                yield buffer3.array();
            }
            case VARCHAR -> {
                String str = (String) value;
                ByteBuffer buffer4 = ByteBuffer.allocate(64);
                buffer4.putInt(str.length());
                buffer4.put(str.getBytes());
                yield buffer4.array();
            }
            case DOUBLE -> {
                ByteBuffer buffer5 = ByteBuffer.allocate(64);
                buffer5.putDouble((double) value);
                yield buffer5.array();
            }
            case UNKNOWN -> {
                ByteBuffer buffer6 = ByteBuffer.allocate(64);

                yield buffer6.array();
            }

            default -> throw new RuntimeException("Unsupported value type: " + type);
        };
    }

    /**
     * 根据给定的字节数组和值类型创建一个 Value 对象。
     *
     * @param bytes 字节数组，表示要转换的值。
     * @param type  值的类型，支持 INTEGER、FLOAT 和 CHAR。
     * @return 转换后的 Value 对象。
     * @throws RuntimeException 如果提供的值类型不受支持。
     */
    public static Value FromByte(byte[] bytes, ValueType type) {
        return switch (type) {
            case INTEGER -> {
                ByteBuffer buffer1 = ByteBuffer.wrap(bytes);
                yield new Value(buffer1.getLong());
            }
            case FLOAT -> {
                ByteBuffer buffer2 = ByteBuffer.wrap(bytes);
                yield new Value(buffer2.getDouble());
            }
            case CHAR -> {
                ByteBuffer buffer3 = ByteBuffer.wrap(bytes);
                var length = buffer3.getInt();
                // int is 4 byte
                String s = new String(bytes, 4, length);
                yield new Value(s);
            }
            case VARCHAR -> {
                ByteBuffer buffer4 = ByteBuffer.wrap(bytes);
                var length = buffer4.getInt();
                String s = new String(bytes, 4, length);
                yield new Value(s);
            }
            case DOUBLE -> {
                ByteBuffer buffer5 = ByteBuffer.wrap(bytes);
                yield new Value(buffer5.getDouble()); // 从字节数组解析为 double 值
            }
            case UNKNOWN -> {
                // 对 UNKNOWN 类型的处理逻辑
                yield new Value(); // 默认设置为 null
            }
            default -> throw new RuntimeException("Unsupported value type: " + type);
        };

    }

    @Override
    public String toString() {
        switch (type) {
            case INTEGER, FLOAT,DOUBLE ->{
                return this.value.toString();
            }
            case CHAR,VARCHAR -> {
                byte[] bytes = ((String) this.value).getBytes();
                ByteBuffer buffer3 = ByteBuffer.wrap(bytes);
                var length = buffer3.getInt();
                // int is 4 byte
                return new String(bytes, 4, length);
            }
            case UNKNOWN -> {
                return "NULL";
            }
            
            default -> throw new RuntimeException("Unsupported value type: " + type);
        }
    }
}
