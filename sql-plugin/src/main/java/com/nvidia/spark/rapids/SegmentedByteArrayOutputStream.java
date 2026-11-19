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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An in-memory output stream that never copies previously written bytes when it grows.
 * Segments are allocated lazily and written out in order without flattening them into one array.
 * Like ByteArrayOutputStream, close is a no-op: closing a codec must leave its output readable.
 * The producer owns the stream until publication; the consumer then owns it exclusively.
 * The transferred owner releases segment references on close; there is no retained buffer pool.
 */
public final class SegmentedByteArrayOutputStream extends OutputStream {
  private final int segmentSize;
  private List<Segment> segments = new ArrayList<>();
  private long count;
  private WritableRegion pending;

  private static final class Segment {
    private final byte[] bytes;
    // Position in the logical byte stream, excluding unused tails of preceding segments.
    private final long logicalOffset;
    // Valid bytes occupy bytes[0, payloadLength); the remaining array capacity is unused.
    private int payloadLength;

    private Segment(int capacity, long logicalOffset) {
      this.bytes = new byte[capacity];
      this.logicalOffset = logicalOffset;
    }
  }

  public SegmentedByteArrayOutputStream(int segmentSize) {
    if (segmentSize <= 0 || segmentSize > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("segmentSize must be between 1 and Integer.MAX_VALUE - 8");
    }
    this.segmentSize = segmentSize;
  }

  private void requireOwned() {
    if (segments == null) {
      throw new IllegalStateException("Record buffer ownership has been transferred");
    }
  }

  private void requireWritable() {
    requireOwned();
    if (pending != null) {
      throw new IllegalStateException("A writable region is still outstanding");
    }
  }

  private Segment writableSegment(int minimumCapacity) {
    requireWritable();
    if (segments.isEmpty() ||
        segmentSize - segments.get(segments.size() - 1).payloadLength < minimumCapacity) {
      segments.add(new Segment(segmentSize, count));
    }
    return segments.get(segments.size() - 1);
  }

  public int getSegmentSize() { return segmentSize; }

  /**
   * Lease contiguous space in the final record. Only one region may be outstanding. The caller
   * may modify only [offset, offset + capacity), and must stop using the array on commit/close.
   * A region never exceeds segmentSize; callers needing larger blocks must use their fallback.
   */
  public WritableRegion acquire(int minimumCapacity) {
    if (minimumCapacity <= 0 || minimumCapacity > segmentSize) {
      throw new IllegalArgumentException("Requested region must fit in one segment");
    }
    WritableRegion region = new WritableRegion(writableSegment(minimumCapacity));
    pending = region;
    return region;
  }

  public final class WritableRegion implements AutoCloseable {
    private final Segment segment;
    private boolean done;

    private WritableRegion(Segment segment) { this.segment = segment; }

    private void requireActive() {
      if (done || pending != this) {
        throw new IllegalStateException("Writable region is no longer active");
      }
    }

    public byte[] array() { requireActive(); return segment.bytes; }
    public int offset() { requireActive(); return segment.payloadLength; }
    public int capacity() { requireActive(); return segmentSize - segment.payloadLength; }

    public void commit(int length) {
      requireActive();
      if (length < 0 || length > capacity()) {
        throw new IndexOutOfBoundsException();
      }
      segment.payloadLength += length;
      count += length;
      close();
    }

    /** Abandon unpublished bytes, including an otherwise empty newly allocated segment. */
    @Override
    public void close() {
      if (!done) {
        requireActive();
        if (segment.payloadLength == 0) segments.remove(segments.size() - 1);
        pending = null;
        done = true;
      }
    }
  }

  @Override
  public void write(int value) {
    Segment segment = writableSegment(1);
    segment.bytes[segment.payloadLength++] = (byte) value;
    count++;
  }

  @Override
  public void write(byte[] bytes, int offset, int length) {
    requireWritable();
    Objects.requireNonNull(bytes, "bytes");
    if (offset < 0 || length < 0 || offset > bytes.length - length) {
      throw new IndexOutOfBoundsException();
    }
    while (length > 0) {
      Segment segment = writableSegment(1);
      int amount = Math.min(length, segmentSize - segment.payloadLength);
      System.arraycopy(bytes, offset, segment.bytes, segment.payloadLength, amount);
      segment.payloadLength += amount;
      count += amount;
      offset += amount;
      length -= amount;
    }
  }

  public long getCount() {
    return count;
  }

  /** Actual backing-array capacity, including unused tails left by contiguous reservations. */
  public long getCapacity() {
    requireOwned();
    return (long) segments.size() * segmentSize;
  }

  /** Write only valid bytes. Does not flush or close the destination or consume this stream. */
  public void writeTo(OutputStream out) throws IOException {
    requireWritable();
    Objects.requireNonNull(out, "out");
    if (out == this) {
      throw new IllegalArgumentException("Cannot write a stream to itself");
    }
    for (Segment segment : segments) {
      out.write(segment.bytes, 0, segment.payloadLength);
    }
  }

  /** A destination that can consume a record without copying its backing arrays. */
  public interface Sink {
    void writeRecord(SegmentedByteArrayOutputStream record) throws IOException;
  }

  /** Transfers the arrays exactly once. The producer can no longer write or read them. */
  public OwnedBuffer takeBuffer() {
    requireWritable();
    OwnedBuffer result = new OwnedBuffer(segments, segmentSize, count);
    segments = null;
    return result;
  }

  /** Immutable payload with exclusive ownership; close promptly drops all array references. */
  public static final class OwnedBuffer implements AutoCloseable {
    private List<Segment> segments;
    private final long count;
    private final long capacity;

    private OwnedBuffer(List<Segment> segments, int segmentSize, long count) {
      this.segments = segments;
      this.count = count;
      this.capacity = (long) segments.size() * segmentSize;
    }

    public long getCount() { return count; }
    public long getCapacity() { return capacity; }

    private void requireOpen() {
      if (segments == null) {
        throw new IllegalStateException("Record buffer is closed");
      }
    }

    public void read(long offset, byte[] target, int start, int length) {
      requireOpen();
      Objects.requireNonNull(target, "target");
      if (start < 0 || length < 0 || start > target.length - length ||
          offset < 0 || offset > count - length) {
        throw new IndexOutOfBoundsException();
      }
      if (length == 0) return;
      int low = 0;
      int high = segments.size() - 1;
      while (low < high) {
        int mid = low + (high - low + 1) / 2;
        if (segments.get(mid).logicalOffset <= offset) low = mid; else high = mid - 1;
      }
      int index = low;
      while (length > 0) {
        Segment segment = segments.get(index++);
        int inSegment = (int) (offset - segment.logicalOffset);
        int size = Math.min(length, segment.payloadLength - inSegment);
        System.arraycopy(segment.bytes, inSegment, target, start, size);
        offset += size;
        start += size;
        length -= size;
      }
    }

    public void writeTo(OutputStream out) throws IOException {
      requireOpen();
      Objects.requireNonNull(out, "out");
      for (Segment segment : segments) {
        out.write(segment.bytes, 0, segment.payloadLength);
      }
    }

    @Override
    public void close() {
      if (segments != null) {
        segments.clear();
        segments = null;
      }
    }
  }

}
