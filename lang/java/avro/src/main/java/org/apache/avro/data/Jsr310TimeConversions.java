/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.data;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class Jsr310TimeConversions {
  public static class DateConversion extends Conversion<LocalDate> {

    @Override
    public Class<LocalDate> getConvertedType() {
      return LocalDate.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "date";
    }

    @Override
    public LocalDate fromInt(Integer daysFromEpoch, Schema schema, LogicalType type) {
      return LocalDate.ofEpochDay(daysFromEpoch);
    }

    @Override
    public Integer toInt(LocalDate date, Schema schema, LogicalType type) {
      long epochDays = date.toEpochDay();

      return (int) epochDays;
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    }
  }

  public static class TimeMillisConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-millis";
    }

    @Override
    public LocalTime fromInt(Integer millisFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(millisFromMidnight));
    }

    @Override
    public Integer toInt(LocalTime time, Schema schema, LogicalType type) {
      return (int) TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay());
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    }
  }

  public static class TimeMicrosConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-micros";
    }

    @Override
    public LocalTime fromLong(Long microsFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos(microsFromMidnight));
    }

    @Override
    public Long toLong(LocalTime time, Schema schema, LogicalType type) {
      return TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay());
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMillisConversion extends Conversion<Instant> {
    @Override
    public Class<Instant> getConvertedType() {
      return Instant.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public Instant fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      return Instant.ofEpochMilli(millisFromEpoch);
    }

    @Override
    public Long toLong(Instant timestamp, Schema schema, LogicalType type) {
      return timestamp.toEpochMilli();
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampMicrosConversion extends Conversion<Instant> {
    @Override
    public Class<Instant> getConvertedType() {
      return Instant.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-micros";
    }

    @Override
    public Instant fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      long epochSeconds = microsFromEpoch / (1_000_000);
      long nanoAdjustment = (microsFromEpoch % (1_000_000)) * 1_000;

      return Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
    }

    @Override
    public Long toLong(Instant instant, Schema schema, LogicalType type) {
      long seconds = instant.getEpochSecond();
      int nanos = instant.getNano();

      if (seconds < 0 && nanos > 0) {
        long micros = Math.multiplyExact(seconds + 1, 1_000_000);
        long adjustment = (nanos / 1_000L) - 1_000_000;

        return Math.addExact(micros, adjustment);
      } else {
        long micros = Math.multiplyExact(seconds, 1_000_000);

        return Math.addExact(micros, nanos / 1_000);
      }
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampLocalDateTimeConversion extends Conversion<LocalDateTime> {
    @Override
    public boolean supportsLogicalTypeWithParams(LogicalType logicalType) {
      if (!(logicalType instanceof LogicalTypes.Timestamp)) {
        return false;
      }
      LogicalTypes.Timestamp timestamp = (LogicalTypes.Timestamp) logicalType;
      return !timestamp.isUtcAdjusted();
    }

    @Override
    public Class<LocalDateTime> getConvertedType() {
      return LocalDateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp";
    }

    @Override
    public LocalDateTime fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      LogicalTypes.Timestamp timestamp = (LogicalTypes.Timestamp) type;
      if ("millis".equals(timestamp.getPrecision())) {
        Instant instant = new Jsr310TimeConversions.TimestampMillisConversion().fromLong(microsFromEpoch, schema, type);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
      }

      if ("micros".equals(timestamp.getPrecision())) {
        Instant instant = new Jsr310TimeConversions.TimestampMicrosConversion().fromLong(microsFromEpoch, schema, type);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
      }

      throw new RuntimeException("Unknown precision");
    }

    @Override
    public Long toLong(LocalDateTime localDateTime, Schema schema, LogicalType type) {
      LogicalTypes.Timestamp timestamp = (LogicalTypes.Timestamp) type;
      Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
      if ("millis".equals(timestamp.getPrecision())) {
        return new Jsr310TimeConversions.TimestampMillisConversion().toLong(instant, schema, type);
      }

      if ("micros".equals(timestamp.getPrecision())) {
        return new Jsr310TimeConversions.TimestampMicrosConversion().toLong(instant, schema, type);
      }

      throw new RuntimeException("Unknown precision");
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestamp("micros", false).addToSchema(Schema.create(Schema.Type.LONG));
    }
  }

  public static class TimestampInstantConversion extends Conversion<Instant> {
    @Override
    public boolean supportsLogicalTypeWithParams(LogicalType logicalType) {
      if (!(logicalType instanceof LogicalTypes.Timestamp)) {
        return false;
      }
      LogicalTypes.Timestamp timestamp = (LogicalTypes.Timestamp) logicalType;
      return timestamp.isUtcAdjusted();
    }

    @Override
    public Class<Instant> getConvertedType() {
      return Instant.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp";
    }

    @Override
    public Instant fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      LogicalTypes.Timestamp timestamp = (LogicalTypes.Timestamp) type;
      if ("millis".equals(timestamp.getPrecision())) {
        return new Jsr310TimeConversions.TimestampMillisConversion().fromLong(microsFromEpoch, schema, type);
      }

      if ("micros".equals(timestamp.getPrecision())) {
        return new Jsr310TimeConversions.TimestampMicrosConversion().fromLong(microsFromEpoch, schema, type);
      }

      throw new RuntimeException("Unknown precision");
    }

    @Override
    public Long toLong(Instant instant, Schema schema, LogicalType type) {
      LogicalTypes.Timestamp timestamp = (LogicalTypes.Timestamp) type;
      if ("millis".equals(timestamp.getPrecision())) {
        return new Jsr310TimeConversions.TimestampMillisConversion().toLong(instant, schema, type);
      }

      if ("micros".equals(timestamp.getPrecision())) {
        return new Jsr310TimeConversions.TimestampMicrosConversion().toLong(instant, schema, type);
      }

      throw new RuntimeException("Unknown precision");
    }

    @Override
    public Schema getRecommendedSchema() {
      return LogicalTypes.timestamp("micros", true).addToSchema(Schema.create(Schema.Type.LONG));
    }
  }
}
