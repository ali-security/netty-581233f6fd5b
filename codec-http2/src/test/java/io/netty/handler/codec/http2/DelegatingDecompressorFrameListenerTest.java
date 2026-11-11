/*
 * Copyright 2026 The Netty Project
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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.SNAPPY;
import static io.netty.handler.codec.http2.Http2Error.FLOW_CONTROL_ERROR;
import static io.netty.handler.codec.http2.Http2Exception.streamError;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DelegatingDecompressorFrameListenerTest {
    private static final int STREAM_ID = 3;
    // SnappyFrameEncoder compresses its input in slices of Short.MAX_VALUE bytes, each one becoming a separate
    // chunk that SnappyFrameDecoder emits as its own decompressed buffer.
    private static final int CHUNK_SIZE = Short.MAX_VALUE;
    private static final int CHUNKS = 8;

    private TrackingAllocator allocator;
    private EmbeddedChannel channel;
    private ChannelHandlerContext ctx;
    private Http2Connection connection;
    private Http2LocalFlowController flowController;
    private Http2FrameListener listener;
    private DelegatingDecompressorFrameListener decompressorListener;

    @BeforeEach
    public void setUp() throws Http2Exception {
        allocator = new TrackingAllocator();
        channel = new EmbeddedChannel();
        // The decompressor EmbeddedChannel shares the config (and so the allocator) of the parent channel.
        channel.config().setAllocator(allocator);
        ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);

        connection = new DefaultHttp2Connection(true);
        flowController = mock(Http2LocalFlowController.class);
        connection.local().flowController(flowController);
        connection.remote().createStream(STREAM_ID, false);

        listener = mock(Http2FrameListener.class);
        decompressorListener = new DelegatingDecompressorFrameListener(connection, listener);
        decompressorListener.onHeadersRead(ctx, STREAM_ID,
                new DefaultHttp2Headers().set(CONTENT_ENCODING, SNAPPY), 0, false);
    }

    @AfterEach
    public void tearDown() {
        Http2Stream stream = connection.stream(STREAM_ID);
        if (stream != null) {
            stream.close();
        }
        channel.finishAndReleaseAll();
    }

    @Test
    public void testDecompressedBuffersReleasedOnSuccess() throws Exception {
        ByteBuf data = snappyFramed("abc", "def");
        try {
            assertEquals(0, decompressorListener.onDataRead(ctx, STREAM_ID, data, 0, true));
            // Every decompressed chunk is delivered on its own, the end of the stream is signalled afterwards.
            verify(listener, times(2)).onDataRead(eq(ctx), eq(STREAM_ID), any(ByteBuf.class), eq(0), eq(false));
            verify(listener).onDataRead(eq(ctx), eq(STREAM_ID), any(ByteBuf.class), eq(0), eq(true));
            assertEquals(1, data.refCnt());
        } finally {
            data.release();
        }
    }

    @Test
    public void testDecompressedBuffersReleasedWhenListenerThrows() throws Exception {
        when(listener.onDataRead(any(ChannelHandlerContext.class), anyInt(), any(ByteBuf.class), anyInt(),
                anyBoolean())).thenThrow(new IllegalStateException("listener failure"));
        assertDecompressedBuffersReleasedOnFailure();
    }

    @Test
    public void testDecompressedBuffersReleasedWhenFlowControllerThrows() throws Exception {
        when(flowController.consumeBytes(any(Http2Stream.class), anyInt()))
                .thenThrow(streamError(STREAM_ID, FLOW_CONTROL_ERROR, "flow control failure"));
        assertDecompressedBuffersReleasedOnFailure();
    }

    private void assertDecompressedBuffersReleasedOnFailure() {
        // A single DATA frame carrying two snappy chunks is decompressed into two buffers, so the second
        // (look-ahead) buffer is already held when the first one is passed to the listener.
        final ByteBuf data = snappyFramed("abc", "def");
        try {
            assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    decompressorListener.onDataRead(ctx, STREAM_ID, data, 0, false);
                }
            });
            // Uncompressed snappy chunks are retained slices of the input, so every decompressed buffer that was
            // not released keeps an extra reference on it.
            assertEquals(1, data.refCnt());
        } finally {
            data.release();
        }
    }

    /**
     * Regression test for CVE-2026-56819. The per-stream decompressor {@link EmbeddedChannel} is closed as soon as
     * a {@code DATA} frame with {@code endOfStream} was processed, but it stays attached to the stream until the
     * stream is removed from the connection. Any {@code DATA} frame which is received in between therefore still
     * reaches the decompressor. Writing into a closed {@link EmbeddedChannel} only fails <em>after</em> the passed
     * in buffer was retained and the channel will not release it in that case, so a remote peer was able to leak
     * one reference of the received buffer per frame and so exhaust the memory of the JVM.
     */
    @Test
    public void dataFramesAfterDecompressorClosedDoNotLeak() throws Exception {
        // Closes (finishes) the decompressor while it is still attached to the stream.
        ByteBuf endOfStreamData = Unpooled.buffer();
        try {
            decompressorListener.onDataRead(ctx, STREAM_ID, endOfStreamData, 0, true);
        } finally {
            if (endOfStreamData.refCnt() > 0) {
                endOfStreamData.release();
            }
        }

        // Replay the same buffer as if it were received via a flood of DATA frames for the already finished
        // stream. Each of them must fail the stream without taking ownership of the buffer, so its reference
        // count must not grow. Without the fix it climbs by one for every frame.
        final ByteBuf data = Unpooled.buffer().writeZero(16);
        try {
            for (int i = 0; i < 8; i++) {
                Http2Exception e = assertThrows(Http2Exception.class, new Executable() {
                    @Override
                    public void execute() throws Throwable {
                        decompressorListener.onDataRead(ctx, STREAM_ID, data, 0, false);
                    }
                });
                assertEquals(Http2Error.INTERNAL_ERROR, e.error());
                assertEquals(1, data.refCnt(), "buffer was retained by DATA frame " + i + " and never released");
            }
        } finally {
            data.release();
        }
    }

    /**
     * Regression test for CVE-2025-58057. A small, highly compressed DATA frame must not be fully inflated and
     * buffered before it is handed to the listener: each decompressed chunk has to be delivered (and released)
     * before the next one is produced.
     */
    @ParameterizedTest
    @ValueSource(ints = { 0, 10 })
    public void decompressedDataIsStreamedToListenerInsteadOfBuffered(final int padding) throws Exception {
        final List<Integer> liveBuffersAtDelivery = new ArrayList<Integer>();
        final ByteBuf received = Unpooled.buffer();
        final boolean[] endOfStreamSeen = new boolean[1];
        when(listener.onDataRead(any(ChannelHandlerContext.class), anyInt(), any(ByteBuf.class), anyInt(),
                anyBoolean())).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) {
                ByteBuf data = (ByteBuf) invocation.getArguments()[2];
                int processed = data.readableBytes() + (Integer) invocation.getArguments()[3];
                if (data.isReadable()) {
                    liveBuffersAtDelivery.add(allocator.liveBuffers());
                    received.writeBytes(data);
                }
                if ((Boolean) invocation.getArguments()[4]) {
                    endOfStreamSeen[0] = true;
                }
                return processed;
            }
        });
        try {
            ByteBuf compressed = snappyCompress(new byte[CHUNKS * CHUNK_SIZE]);
            final int compressedBytes = compressed.readableBytes();
            assertTrue(compressedBytes < CHUNK_SIZE, "payload must be highly compressed: " + compressedBytes);

            int returned;
            try {
                returned = decompressorListener.onDataRead(ctx, STREAM_ID, compressed, padding, true);
            } finally {
                compressed.release();
            }

            assertEquals(CHUNKS, liveBuffersAtDelivery.size());
            for (int live : liveBuffersAtDelivery) {
                // Only the chunk currently handed to the listener may be alive. Before the fix every chunk of the
                // frame was decompressed into the EmbeddedChannel's inbound queue before the first one was delivered.
                assertEquals(1, live, "decompressed buffers alive at delivery: " + liveBuffersAtDelivery);
            }
            assertEquals(0, allocator.liveBuffers());
            assertEquals(CHUNKS * CHUNK_SIZE, received.readableBytes());
            assertTrue(endOfStreamSeen[0]);

            // All compressed bytes (including padding) must be returned to the flow controller exactly once.
            ArgumentCaptor<Integer> consumed = ArgumentCaptor.forClass(Integer.class);
            verify(flowController, atLeastOnce()).consumeBytes(eq(connection.stream(STREAM_ID)), consumed.capture());
            int totalConsumed = returned;
            for (int bytes : consumed.getAllValues()) {
                totalConsumed += bytes;
            }
            assertEquals(compressedBytes + padding, totalConsumed);
        } finally {
            received.release();
        }
    }

    @Test
    public void decompressedBufferIsReleasedWhenListenerThrows() throws Exception {
        final Http2Exception failure = streamError(STREAM_ID, Http2Error.INTERNAL_ERROR, "boom");
        when(listener.onDataRead(any(ChannelHandlerContext.class), anyInt(), any(ByteBuf.class), anyInt(),
                anyBoolean())).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                ByteBuf data = (ByteBuf) invocation.getArguments()[2];
                if (data.isReadable()) {
                    throw failure;
                }
                return (Integer) invocation.getArguments()[3];
            }
        });

        final ByteBuf compressed = snappyCompress(new byte[CHUNKS * CHUNK_SIZE]);
        try {
            Http2Exception thrown = assertThrows(Http2Exception.class, new Executable() {
                @Override
                public void execute() throws Throwable {
                    decompressorListener.onDataRead(ctx, STREAM_ID, compressed, 0, false);
                }
            });
            assertSame(failure, thrown);
        } finally {
            compressed.release();
        }
        assertFalse(allocator.allocated.isEmpty());
        // Neither the chunk handed to the failing listener nor any chunk decompressed afterwards may be retained.
        assertEquals(0, allocator.liveBuffers());
    }

    private static ByteBuf snappyCompress(byte[] data) {
        EmbeddedChannel encoder = new EmbeddedChannel(new SnappyFrameEncoder());
        try {
            assertTrue(encoder.writeOutbound(Unpooled.wrappedBuffer(data)));
            ByteBuf compressed = Unpooled.buffer();
            for (;;) {
                ByteBuf buf = encoder.readOutbound();
                if (buf == null) {
                    break;
                }
                compressed.writeBytes(buf);
                buf.release();
            }
            return compressed;
        } finally {
            encoder.finishAndReleaseAll();
        }
    }

    private static ByteBuf snappyFramed(String... chunks) {
        ByteBuf framed = Unpooled.buffer();
        // Stream identifier chunk.
        framed.writeByte(0xff).writeMediumLE(6).writeBytes("sNaPpY".getBytes(CharsetUtil.US_ASCII));
        for (String chunk : chunks) {
            byte[] bytes = chunk.getBytes(CharsetUtil.US_ASCII);
            // Uncompressed data chunk: type, length (checksum + data), checksum (not validated) and data.
            framed.writeByte(0x01).writeMediumLE(4 + bytes.length).writeIntLE(0).writeBytes(bytes);
        }
        return framed;
    }

    private static final class TrackingAllocator extends AbstractByteBufAllocator {
        final List<ByteBuf> allocated = new ArrayList<ByteBuf>();

        TrackingAllocator() {
            super(false);
        }

        @Override
        protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.heapBuffer(initialCapacity, maxCapacity);
            allocated.add(buf);
            return buf;
        }

        @Override
        protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            ByteBuf buf = UnpooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity, maxCapacity);
            allocated.add(buf);
            return buf;
        }

        @Override
        public boolean isDirectBufferPooled() {
            return false;
        }

        int liveBuffers() {
            int live = 0;
            for (ByteBuf buf : allocated) {
                if (buf.refCnt() > 0) {
                    live++;
                }
            }
            return live;
        }
    }
}
