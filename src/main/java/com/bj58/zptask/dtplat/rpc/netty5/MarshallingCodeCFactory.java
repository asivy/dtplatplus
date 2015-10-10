package com.bj58.zptask.dtplat.rpc.netty5;

import io.netty.handler.codec.marshalling.DefaultMarshallerProvider;
import io.netty.handler.codec.marshalling.DefaultUnmarshallerProvider;
import io.netty.handler.codec.marshalling.MarshallingDecoder;
import io.netty.handler.codec.marshalling.MarshallingEncoder;

import org.jboss.marshalling.MarshallerFactory;
import org.jboss.marshalling.Marshalling;
import org.jboss.marshalling.MarshallingConfiguration;

/**
 * JBOSS解码器  快速  简历  高效
 *
 *
 * @author WuTong
 * @version 1.0
 * @date  2015年9月11日 下午1:50:07
 * @see 
 * @since
 */
public final class MarshallingCodeCFactory {

    /**
     * 创建Jboss Marshalling解码器MarshallingDecoder
     * 
     * @return
     */
    public static MarshallingDecoder buildMarshallingDecoder() {
        final MarshallerFactory factory = createMarshallerFactory();
        final MarshallingConfiguration config = createMarshallingConfig();
        MarshallingDecoder decoder = new MarshallingDecoder(new DefaultUnmarshallerProvider(factory, config));
        return decoder;
    }

    /** 
     * 创建Jboss Marshalling编码器MarshallingEncoder
     * 
     * @return
     */
    public static MarshallingEncoder buildMarshallingEncoder() {
        final MarshallerFactory factory = createMarshallerFactory();
        final MarshallingConfiguration config = createMarshallingConfig();
        MarshallingEncoder encoder = new MarshallingEncoder(new DefaultMarshallerProvider(factory, config));
        return encoder;
    }

    private static MarshallerFactory createMarshallerFactory() {
        return Marshalling.getProvidedMarshallerFactory("serial");
    }

    private static MarshallingConfiguration createMarshallingConfig() {
        final MarshallingConfiguration configuration = new MarshallingConfiguration();
        configuration.setVersion(5);
        return configuration;
    }
}
