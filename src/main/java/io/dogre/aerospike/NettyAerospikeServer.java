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

/**
 * Implementation of {@link AerospikeServer} based on Netty.
 *
 * @author dogre
 */
public class NettyAerospikeServer implements AerospikeServer {

    private static final Logger logger = LoggerFactory.getLogger(NettyAerospikeServer.class);

    /**
     * The number of IO threads.
     */
    private int ioThreads;

    /**
     * The number of Worker threads.
     */
    private int workerThreads;

    /**
     * Whether stared.
     */
    private boolean started;

    /**
     * Constructor.
     *
     * @param ioThreads The number of IO threads.
     * @param workerThreads The number of Worker threads.
     */
    public NettyAerospikeServer(int ioThreads, int workerThreads) {
        this.ioThreads = ioThreads;
        this.workerThreads = workerThreads;
        this.started = false;
    }

    @Override
    public void start(String host, int port, String[] namespaces) {
        logger.info("Starting server : host = {}, port = {}, namespaces = {}, " +
                        "# of io threads = {}, # of worker threads = {}", host, port, namespaces, this.ioThreads,
                this.workerThreads);

        // create ServiceHandler
        ServiceHandler serviceHandler = new ServiceHandlerImpl(host + ":" + port, namespaces);

        EventLoopGroup ioGroup = new NioEventLoopGroup(this.ioThreads);
        EventLoopGroup workerGroup = new NioEventLoopGroup(this.workerThreads);
        ChannelHandler channelHandler = new AerospikeServiceChannelHandler(serviceHandler);
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

            ChannelFuture channelFuture = bootstrap.bind(port).sync();
            this.started = channelFuture.isSuccess();
            logger.info("Server started");
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
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            ByteBuf byteBuf = (ByteBuf) msg;
            this.buffer.writeBytes(byteBuf);
            byteBuf.release();

            // The first 8 bytes of Aerospike Message contains the infomation of message size.
            // The first byte is the version of message protocol.
            // The second byte is the type of message, 1 means 'info', 3 means 'command'.
            // The rest 6 bytes is the length of message.
            if (8 <= this.buffer.readableBytes()) {
                long sizeHeader = this.buffer.getLong(this.buffer.readerIndex());
                int length = (int) (sizeHeader & 0xffffffffffffL);
                if (8 + length <= this.buffer.readableBytes()) {
                    byte[] request = new byte[8 + length];
                    this.buffer.readBytes(request);

                    byte[] response = this.serviceHandler.handleRequest(request);
                    ByteBuf responseByteBuf = ByteBufAllocator.DEFAULT.buffer(response.length);
                    responseByteBuf.writeBytes(response);

                    ctx.writeAndFlush(responseByteBuf);
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("Exception", cause);
            ctx.close();
        }

    }

}