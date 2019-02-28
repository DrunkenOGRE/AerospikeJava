package io.dogre.aerospike;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServer implements Server {

    private int port;

    private int ioThreads;

    private int workerThreads;

    private ServiceHandler serviceHandler;

    private boolean started;

    public NettyServer(int port, int ioThreads, int workerThreads, ServiceHandler serviceHandler) {
        this.port = port;
        this.ioThreads = ioThreads;
        this.workerThreads = workerThreads;
        this.serviceHandler = serviceHandler;
        this.started = false;
    }

    @Override
    public void start() {
        EventLoopGroup ioGroup = new NioEventLoopGroup(this.ioThreads);
        EventLoopGroup workerGroup = new NioEventLoopGroup(this.workerThreads);
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
            this.started = channelFuture.isSuccess();
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            ioGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    @Override
    public boolean isStarted() {
        return this.started;
    }

    @ChannelHandler.Sharable
    public static class AerospikeServiceChannelHandler extends ChannelInboundHandlerAdapter {

        private static final Logger logger = LoggerFactory.getLogger(AerospikeServiceChannelHandler.class);

        private ServiceHandler serviceHandler;

        private ByteBuf buffer;

        public static final int BUFFER_SIZE = 96;

        public AerospikeServiceChannelHandler(ServiceHandler serviceHandler) {
            this.serviceHandler = serviceHandler;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            this.buffer = ctx.alloc().buffer(BUFFER_SIZE);
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
            if (this.buffer != null) {
                this.buffer.release();
                this.buffer = null;
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            this.buffer.writeBytes(byteBuf);
            byteBuf.release();

            long sizeHeader = this.buffer.getLong(this.buffer.readerIndex());
            int length = (int) (sizeHeader & 0xffffffffffffL);
            if (length <= this.buffer.readableBytes()) {
                byte[] request = new byte[8 + length];
                this.buffer.readBytes(request);

                byte[] response = this.serviceHandler.handleRequest(request);
                ByteBuf responseByteBuf = ByteBufAllocator.DEFAULT.buffer(response.length);
                responseByteBuf.writeBytes(response);

                ctx.writeAndFlush(responseByteBuf);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Exception", cause);
            ctx.close();
        }

    }

}