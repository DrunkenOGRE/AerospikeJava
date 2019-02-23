package io.dogre.aerospike;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer implements Server {

    private int port;

    private int ioThreads;

    private int workerThreads;

    private ServiceHandler serviceHandler;

    public NettyServer(int port, int ioThreads, int workerThreads, ServiceHandler serviceHandler) {
        this.port = port;
        this.ioThreads = ioThreads;
        this.workerThreads = workerThreads;
        this.serviceHandler = serviceHandler;
    }

    @Override
    public void start() {
        EventLoopGroup ioGroup = new NioEventLoopGroup(this.ioThreads);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ChannelHandler channelHandler = new AerospikeServiceChannelHandler(this.serviceHandler);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(ioGroup, workerGroup).channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {             //송수신 되는 데이터 가공 핸들러
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                            pipeline.addLast(channelHandler);
                        }
                    });

            ChannelFuture channelFuture = bootstrap.bind(this.port).sync();
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            ioGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @ChannelHandler.Sharable
    public static class AerospikeServiceChannelHandler extends ChannelInboundHandlerAdapter {

        private static final Logger logger = LoggerFactory.getLogger(ChannelHandler.class);

        private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        private ServiceHandler serviceHandler;

        public AerospikeServiceChannelHandler(ServiceHandler serviceHandler) {
            this.serviceHandler = serviceHandler;
        }

        @Override
        public void channelActive(ChannelHandlerContext context) throws Exception {
            this.channels.add(context.channel());
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) throws Exception {
            ByteBuf requestByteBuf = (ByteBuf) message;
            byte[] request = new byte[requestByteBuf.readableBytes()];
            requestByteBuf.readBytes(request);

            byte[] response = this.serviceHandler.handleRequest(request);
            ByteBuf responseByteBuf = ByteBufAllocator.DEFAULT.buffer(response.length);
            responseByteBuf.writeBytes(response);
            this.channels.writeAndFlush(responseByteBuf);
        }

    }

}