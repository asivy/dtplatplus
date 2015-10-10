package com.bj58.zptask.dtplat.core.compiler;

/**
 * Compiler. (SPI, Singleton, ThreadSafe)
 */
public interface Compiler {

    /**
     * Compile java source code.
     *
     * @param code
     *            Java source code
     * @param classLoader
     * @return Compiled class
     */
    Class<?> compile(String code, ClassLoader classLoader);

}
