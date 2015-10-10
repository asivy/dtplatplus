package com.bj58.zptask.dtplat.core.compiler.support;

import com.bj58.zptask.dtplat.commons.InjectorHolder;
import com.bj58.zptask.dtplat.core.compiler.Compiler;

/**
 * AdaptiveCompiler. (SPI, Singleton, ThreadSafe)
 */
public class AdaptiveCompiler implements com.bj58.zptask.dtplat.core.compiler.Compiler {

    private static volatile String DEFAULT_COMPILER;

    public static void setDefaultCompiler(String compiler) {
        DEFAULT_COMPILER = compiler;
    }

    public Class<?> compile(String code, ClassLoader classLoader) {
        Compiler compiler;
        //        ExtensionLoader<Compiler> loader = ExtensionLoader.getExtensionLoader(Compiler.class);
        //        String name = DEFAULT_COMPILER; // copy reference
        //        if (name != null && name.length() > 0) {
        //            compiler = loader.getExtension(name);
        //        } else {
        //            compiler = loader.getDefaultExtension();
        //        }
        compiler = InjectorHolder.getInstance(Compiler.class);
        return compiler.compile(code, classLoader);
    }

}
