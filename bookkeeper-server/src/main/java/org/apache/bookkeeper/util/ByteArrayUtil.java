package org.apache.bookkeeper.util;

public class ByteArrayUtil {

    public static final boolean isArrayAllZeros(final byte[] array) {
        int sum = 0;
        for (byte b : array) {
            sum |= b;
        }

        return sum == 0;
    }
}
