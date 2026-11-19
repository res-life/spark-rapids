/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.rapids;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.zip.Checksum;

import net.jpountz.lz4.LZ4Compressor;

/**
 * Writes the lz4-java block stream used by Spark directly into record-owned heap segments.
 * Compressed payloads have no intermediate output array. Input buffering, the RAW-block copy,
 * and any copying inside the compressor/JNI implementation remain.
 */
public final class Lz4SegmentedOutputStream extends OutputStream {
  private static final byte[] MAGIC = {'L', 'Z', '4', 'B', 'l', 'o', 'c', 'k'};
  private static final int HEADER_LENGTH = 21;
  private static final int RAW = 0x10;
  private static final int LZ4 = 0x20;
  private static final int LEVEL_BASE = 10;
  private final SegmentedByteArrayOutputStream out;
  private final LZ4Compressor compressor;
  private final Checksum checksum;
  private final byte[] input;
  private final int level;
  private int count;
  private boolean finished;

  public static boolean supports(int blockSize, int segmentSize, LZ4Compressor compressor) {
    return blockSize >= 64 && blockSize <= (1 << 25) &&
        HEADER_LENGTH + compressor.maxCompressedLength(blockSize) <= segmentSize;
  }

  public Lz4SegmentedOutputStream(SegmentedByteArrayOutputStream out, int blockSize,
      LZ4Compressor compressor, Checksum checksum) {
    this.out = Objects.requireNonNull(out, "out");
    this.compressor = Objects.requireNonNull(compressor, "compressor");
    this.checksum = Objects.requireNonNull(checksum, "checksum");
    if (!supports(blockSize, out.getSegmentSize(), compressor)) {
      throw new IllegalArgumentException("Encoded LZ4 block must fit in one segment");
    }
    input = new byte[blockSize];
    level = Math.max(0, 32 - Integer.numberOfLeadingZeros(blockSize - 1) - LEVEL_BASE);
  }

  private void requireOpen() throws IOException {
    if (finished) throw new IOException("LZ4 stream is finished");
  }

  @Override
  public void write(int value) throws IOException {
    requireOpen();
    input[count++] = (byte) value;
    if (count == input.length) emitBlock();
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    requireOpen();
    Objects.requireNonNull(bytes, "bytes");
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IndexOutOfBoundsException();
    }
    while (length > 0) {
      int amount = Math.min(length, input.length - count);
      System.arraycopy(bytes, offset, input, count, amount);
      count += amount;
      offset += amount;
      length -= amount;
      if (count == input.length) emitBlock();
    }
  }

  private void emitBlock() {
    if (count == 0) return;
    checksum.reset();
    checksum.update(input, 0, count);
    int check = (int) checksum.getValue();
    int bound = compressor.maxCompressedLength(count);
    try (SegmentedByteArrayOutputStream.WritableRegion region =
        out.acquire(HEADER_LENGTH + bound)) {
      byte[] bytes = region.array();
      int offset = region.offset();
      int length = compressor.compress(input, 0, count,
          bytes, offset + HEADER_LENGTH, bound);
      int method = LZ4;
      if (length >= count) {
        method = RAW;
        length = count;
        System.arraycopy(input, 0, bytes, offset + HEADER_LENGTH, count);
      }
      writeHeader(bytes, offset, method, length, count, check);
      region.commit(HEADER_LENGTH + length);
    }
    count = 0;
  }

  private void writeHeader(byte[] bytes, int offset, int method,
      int encodedLength, int decodedLength, int check) {
    System.arraycopy(MAGIC, 0, bytes, offset, MAGIC.length);
    bytes[offset + MAGIC.length] = (byte) (method | level);
    writeInt(bytes, offset + 9, encodedLength);
    writeInt(bytes, offset + 13, decodedLength);
    writeInt(bytes, offset + 17, check);
  }

  private static void writeInt(byte[] bytes, int offset, int value) {
    for (int index = 0; index < 4; index++) {
      bytes[offset + index] = (byte) (value >>> (8 * index));
    }
  }

  /** Spark uses syncFlush=false: flushing must not end a partial compression block. */
  @Override
  public void flush() throws IOException { out.flush(); }

  public void finish() throws IOException {
    requireOpen();
    emitBlock();
    try (SegmentedByteArrayOutputStream.WritableRegion region = out.acquire(HEADER_LENGTH)) {
      writeHeader(region.array(), region.offset(), RAW, 0, 0, 0);
      region.commit(HEADER_LENGTH);
    }
    finished = true;
    out.flush();
  }

  @Override
  public void close() throws IOException {
    if (!finished) finish();
    out.close();
  }
}
