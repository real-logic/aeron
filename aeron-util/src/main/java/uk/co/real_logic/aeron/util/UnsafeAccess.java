package uk.co.real_logic.aeron.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

public class UnsafeAccess {

    public static final Unsafe UNSAFE;

    static
    {
        try
        {
            final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>()
            {
                public Unsafe run() throws Exception
                {
                    final Field f = Unsafe.class.getDeclaredField("theUnsafe");
                    f.setAccessible(true);
                    return (Unsafe)f.get(null);
                }
            };

            UNSAFE = AccessController.doPrivileged(action);
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

}
