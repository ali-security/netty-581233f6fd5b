/*
 * Copyright 2019 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayDeque;
import java.util.Queue;

import static io.netty.handler.codec.http2.Http2CodecUtil.DEFAULT_MAX_FRAME_SIZE;
import static io.netty.handler.codec.http2.Http2CodecUtil.FRAME_HEADER_LENGTH;
import static io.netty.handler.codec.http2.Http2CodecUtil.connectionPrefaceBuf;
import static io.netty.handler.codec.http2.Http2Error.CANCEL;
import static io.netty.handler.codec.http2.Http2Error.ENHANCE_YOUR_CALM;
import static io.netty.handler.codec.http2.Http2Error.NO_ERROR;
import static io.netty.handler.codec.http2.Http2Error.PROTOCOL_ERROR;
import static io.netty.handler.codec.http2.Http2FrameTypes.GO_AWAY;
import static io.netty.handler.codec.http2.Http2FrameTypes.RST_STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Http2MaxRstFrameLimitEncoder}.
 */
public class Http2MaxRstFrameLimitEncoderTest {

    private Http2MaxRstFrameLimitEncoder encoder;

    @Mock
    private Http2FrameWriter writer;

    @Mock
    private ChannelHandlerContext ctx;

    @Mock
    private Channel channel;

    @Mock
    private Channel.Unsafe unsafe;

    @Mock
    private ChannelConfig config;

    @Mock
    private EventExecutor executor;

    private final Queue<ChannelPromise> goAwayPromises = new ArrayDeque<ChannelPromise>();

    /**
     * Init fields and do mocking.
     */
    @BeforeEach
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        Http2FrameWriter.Configuration configuration = mock(Http2FrameWriter.Configuration.class);
        Http2FrameSizePolicy frameSizePolicy = mock(Http2FrameSizePolicy.class);
        when(writer.configuration()).thenReturn(configuration);
        when(configuration.frameSizePolicy()).thenReturn(frameSizePolicy);
        when(frameSizePolicy.maxFrameSize()).thenReturn(DEFAULT_MAX_FRAME_SIZE);

        when(writer.writeRstStream(eq(ctx), anyInt(), anyLong(), any(ChannelPromise.class)))
                .thenAnswer(new Answer<ChannelFuture>() {
                    @Override
                    public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                        return handlePromise(invocationOnMock, 3);
                    }
                });
        when(writer.writeGoAway(any(ChannelHandlerContext.class), anyInt(), anyLong(), any(ByteBuf.class),
                any(ChannelPromise.class))).thenAnswer(new Answer<ChannelFuture>() {
            @Override
            public ChannelFuture answer(InvocationOnMock invocationOnMock) {
                ReferenceCountUtil.release(invocationOnMock.getArgument(3));
                ChannelPromise promise = invocationOnMock.getArgument(4);
                goAwayPromises.offer(promise);
                return promise;
            }
        });
        Http2Connection connection = new DefaultHttp2Connection(false);
        connection.remote().flowController(new DefaultHttp2RemoteFlowController(connection));
        connection.local().flowController(new DefaultHttp2LocalFlowController(connection).frameWriter(writer));

        DefaultHttp2ConnectionEncoder defaultEncoder =
                new DefaultHttp2ConnectionEncoder(connection, writer);
        encoder = new Http2MaxRstFrameLimitEncoder(defaultEncoder, 2, 1);
        DefaultHttp2ConnectionDecoder decoder =
                new DefaultHttp2ConnectionDecoder(connection, encoder, mock(Http2FrameReader.class));
        Http2ConnectionHandler handler = new Http2ConnectionHandlerBuilder()
                .frameListener(mock(Http2FrameListener.class))
                .codec(decoder, encoder).build();

        // Set LifeCycleManager on encoder and decoder
        when(ctx.channel()).thenReturn(channel);
        when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
        when(channel.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
        when(executor.inEventLoop()).thenReturn(true);
        doAnswer(new Answer<ChannelPromise>() {
            @Override
            public ChannelPromise answer(InvocationOnMock invocation) throws Throwable {
                return newPromise();
            }
        }).when(ctx).newPromise();
        when(ctx.executor()).thenReturn(executor);
        when(channel.isActive()).thenReturn(false);
        when(channel.config()).thenReturn(config);
        when(channel.isWritable()).thenReturn(true);
        when(channel.bytesBeforeUnwritable()).thenReturn(Long.MAX_VALUE);
        when(config.getWriteBufferHighWaterMark()).thenReturn(Integer.MAX_VALUE);
        when(config.getMessageSizeEstimator()).thenReturn(DefaultMessageSizeEstimator.DEFAULT);
        ChannelMetadata metadata = new ChannelMetadata(false, 16);
        when(channel.metadata()).thenReturn(metadata);
        when(channel.unsafe()).thenReturn(unsafe);
        handler.handlerAdded(ctx);
    }

    private ChannelPromise handlePromise(InvocationOnMock invocationOnMock, int promiseIdx) {
        ChannelPromise promise = invocationOnMock.getArgument(promiseIdx);
        return promise.setSuccess();
    }

    @AfterEach
    public void teardown() {
        // Close and release any buffered frames.
        encoder.close();

        // Notify all goAway ChannelPromise instances now as these will also release the retained ByteBuf for the
        // debugData.
        for (;;) {
            ChannelPromise promise = goAwayPromises.poll();
            if (promise == null) {
                break;
            }
            promise.setSuccess();
        }
    }

    @ParameterizedTest
    @EnumSource(Http2Error.class)
    public void testLimitRst(Http2Error error) {
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        verifyFlushAndClose(0, false);
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        if (error == CANCEL || error == NO_ERROR) {
            // CANCEL and NO_ERROR are ignored as these will not be caused by a stream error.
            verifyFlushAndClose(0, false);
        } else {
            verifyFlushAndClose(1, true);
        }
    }

    @ParameterizedTest
    @EnumSource(Http2Error.class)
    public void testLimitRstReset(Http2Error error) throws Exception {
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        verifyFlushAndClose(0, false);
        Thread.sleep(1000);
        assertTrue(encoder.writeRstStream(ctx, 1, error.code(), newPromise()).isSuccess());
        verifyFlushAndClose(0, false);
    }

    @Test
    public void testServerClosesConnectionWhenPeerTriggersRstFlood() {
        int maxRstFrames = 5;
        EmbeddedChannel channel = new EmbeddedChannel(new Http2ConnectionHandlerBuilder()
                .server(true)
                .frameListener(new Http2FrameAdapter())
                .encoderEnforceMaxRstFramesPerWindow(maxRstFrames, 60)
                .build());
        assertPeerTriggeredRstFloodClosesConnection(channel, maxRstFrames);
    }

    @Test
    public void testServerLimitsPeerTriggeredRstFramesByDefault() {
        EmbeddedChannel channel = new EmbeddedChannel(new Http2ConnectionHandlerBuilder()
                .server(true)
                .frameListener(new Http2FrameAdapter())
                .build());
        // Servers enforce a default limit of 200 RST frames per 30 seconds.
        assertPeerTriggeredRstFloodClosesConnection(channel, 200);
    }

    private static void assertPeerTriggeredRstFloodClosesConnection(EmbeddedChannel channel, int maxRstFrames) {
        try {
            Http2FrameInboundWriter inboundWriter = new Http2FrameInboundWriter(channel);
            channel.writeInbound(connectionPrefaceBuf());
            inboundWriter.writeInboundSettings(new Http2Settings());
            inboundWriter.writeInboundSettingsAck();

            int streamId = 1;
            for (int i = 0; i < maxRstFrames; i++) {
                triggerServerRstStream(inboundWriter, streamId);
                streamId += 2;
            }
            ByteBuf outbound = readOutbound(channel);
            try {
                assertEquals(maxRstFrames, countFrames(outbound, RST_STREAM, PROTOCOL_ERROR.code()));
                assertEquals(0, countFrames(outbound, GO_AWAY, ENHANCE_YOUR_CALM.code()));
            } finally {
                outbound.release();
            }
            assertTrue(channel.isActive());

            // One more RST_STREAM generated by the server within the window must tear down the connection.
            triggerServerRstStream(inboundWriter, streamId);
            outbound = readOutbound(channel);
            try {
                assertEquals(1, countFrames(outbound, RST_STREAM, PROTOCOL_ERROR.code()));
                assertEquals(1, countFrames(outbound, GO_AWAY, ENHANCE_YOUR_CALM.code()));
            } finally {
                outbound.release();
            }
            assertFalse(channel.isActive());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Opens a new stream and then sends a frame that is invalid for it, which makes the server reset the stream.
     */
    private static void triggerServerRstStream(Http2FrameInboundWriter inboundWriter, int streamId) {
        inboundWriter.writeInboundHeaders(streamId, new DefaultHttp2Headers()
                .method("GET").scheme("http").authority("localhost").path("/"), 0, false);
        // A WINDOW_UPDATE with an increment of 0 is a stream error (PROTOCOL_ERROR).
        ByteBuf payload = Unpooled.buffer(4).writeInt(0);
        inboundWriter.writeInboundFrame(Http2FrameTypes.WINDOW_UPDATE, streamId, new Http2Flags(), payload);
    }

    private static ByteBuf readOutbound(EmbeddedChannel channel) {
        ByteBuf frames = Unpooled.buffer();
        for (;;) {
            ByteBuf buf = channel.readOutbound();
            if (buf == null) {
                return frames;
            }
            frames.writeBytes(buf);
            buf.release();
        }
    }

    private static int countFrames(ByteBuf frames, byte frameType, long errorCode) {
        int count = 0;
        int index = frames.readerIndex();
        while (frames.writerIndex() - index >= FRAME_HEADER_LENGTH) {
            int payloadLength = frames.getUnsignedMedium(index);
            int payloadIndex = index + FRAME_HEADER_LENGTH;
            if (frames.getByte(index + 3) == frameType) {
                // RST_STREAM carries the error code first, GO_AWAY carries it after the last stream id.
                int errorCodeIndex = frameType == GO_AWAY ? payloadIndex + 4 : payloadIndex;
                if (frames.getUnsignedInt(errorCodeIndex) == errorCode) {
                    count++;
                }
            }
            index = payloadIndex + payloadLength;
        }
        return count;
    }

    private void verifyFlushAndClose(int invocations, boolean failed) {
        verify(ctx, atLeast(invocations)).flush();
        verify(ctx, times(invocations)).close();
        if (failed) {
            verify(writer, times(1)).writeGoAway(eq(ctx), eq(Integer.MAX_VALUE), eq(ENHANCE_YOUR_CALM.code()),
                    any(ByteBuf.class), any(ChannelPromise.class));
        }
    }

    private ChannelPromise newPromise() {
        return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    }
}
