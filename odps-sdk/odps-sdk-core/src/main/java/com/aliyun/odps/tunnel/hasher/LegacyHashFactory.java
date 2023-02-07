package com.aliyun.odps.tunnel.hasher;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

class LegacyHashFactory implements HasherFactory {

    enum HasherEntry {
        TINYINT("tinyint", new IntegerHasher()),
        SMALLINT("smallint", new IntegerHasher()),
        INT("int", new IntegerHasher()),
        BIGINT("bigint", new BigintHasher()),
        DATETIME("datetime", new BigintHasher()),
        DATE("date", new DateHasher()),
        INTERVALYEARMONTH("intervalyearmonth", new IntervalYearMonthHasher()),
        FLOAT("float", new FloatHasher()),
        DOUBLE("double", new DoubleHasher()),
        BOOLEAN("boolean", new BooleanHasher()),
        CHAR("char", new StringHasher()),
        VARCHAR("varchar", new StringHasher()),
        STRING("string", new StringHasher()),
        BINARY("binary", new StringHasher()),
        TIMESTAMP("timestamp", new TimestampHasher()),
        INTERVALDAYTIME("intervaldaytime", new IntervalDayTimeHasher()),
        DECIMAL("decimal", new DecimalHasher()),;

        String typeName;
        OdpsHasher hasher;

        HasherEntry(String typeName, OdpsHasher hasher) {
            this.typeName = typeName;
            this.hasher = hasher;
        }

        public String getTypeName() {
            return typeName;
        }

        public OdpsHasher getHasher() {
            return hasher;
        }
    }

    /**
     * all type hash
     */
    private static Map<String, OdpsHasher> factoryMap = new HashMap<>();

    static {
        for (HasherEntry entry : HasherEntry.values()) {
            factoryMap.put(entry.getTypeName(), entry.getHasher());
        }
    }


    @Override
    public OdpsHasher getHasher(String type) {
        return factoryMap.get(type);
    }

    public String getName() { return "legacy"; }

    /*
     * basic hash function for long
     */
    static int basicLongHasher(long val) {
        long l = val;
        l = (l >> 32 ) ^ l;
        return (int) (l);
    }

    /**
     * Integer type hash
     */
    private static class IntegerHasher implements OdpsHasher<Integer> {
        @Override
        public int hash(Integer val) {
            if (val == null) {
                return 0;
            }
            return basicLongHasher(val.longValue());
        }
    }

    /**
     * Bigint type hash
     */
    private static class BigintHasher implements OdpsHasher<Long> {
        @Override
        public int hash(Long val) {
            if (val == null) {
                return 0;
            }
            return basicLongHasher(val.longValue());
        }
    }

    /**
     * Float type hash
     */
    private static class FloatHasher implements OdpsHasher<Float> {
        @Override
        public int hash(Float val) {
            if (val == null) {
                return 0;
            }
            return basicLongHasher((long)Float.floatToIntBits(val));
        }
    }

    /**
     * Double type hash
     */
    private static class DoubleHasher implements OdpsHasher<Double> {
        @Override
        public int hash(Double val) {
            if (val == null) {
                return 0;
            }
            return basicLongHasher(Double.doubleToLongBits(val));
        }
    }

    /**
     * Boolean type hash
     */
    private static class BooleanHasher implements OdpsHasher<Boolean> {
        @Override
        public int hash(Boolean val) {
            if (val == null) {
                return 0;
            }
            //it's magic number
            if (val) {
                return 0x172ba9c7;
            } else {
                return -0x3a59cb12;
            }
        }
    }

    /**
     * String type hash
     */
    private static class StringHasher implements OdpsHasher<String> {
        private static final Charset UTF8 = Charset.forName("UTF8");
        @Override
        public int hash(String val) {
            if (val == null) {
                return 0;
            }

            byte[] chars = val.getBytes(UTF8);
            int hashVal = 0;
            for (int i = 0; i < chars.length; i++) {
                hashVal = hashVal * 31 + chars[i];
            }
            return hashVal;
        }
    }

    /**
     * Date type hash
     */
    private static class DateHasher extends BigintHasher {
    }

    /**
     * Datetime type hash
     */
    private static class TimeHasher extends BigintHasher {
    }

    /**
     * Interval year to month type hash
     */
    private static class IntervalYearMonthHasher extends BigintHasher {
    }

    /**
     * Timestamp type hash
     */
    private static class TimestampHasher implements OdpsHasher<Timestamp> {
        @Override
        public int hash(Timestamp val) {
            if (val == null) {
                return 0;
            }
            long millis = val.getTime();
            long seconds = (millis >= 0 ? millis : millis - 999) / 1000;
            int nanos = val.getNanos();
            seconds <<= 30;
            seconds |= nanos;
            return basicLongHasher(seconds);
        }
    }

    /**
     * Interval Day to time type hash
     */
    private static class IntervalDayTimeHasher implements OdpsHasher<BigDecimal> {
        @Override
        public int hash(BigDecimal val) {
            if (val == null) {
                return 0;
            }
            BigDecimal[] divideAndRemainder =
                    val.divideAndRemainder(new BigDecimal(1).scaleByPowerOfTen(9));
            long totalSec = divideAndRemainder[0].longValue();
            int nanos = divideAndRemainder[1].intValue();
            totalSec <<= 30;
            totalSec |= nanos;
            return basicLongHasher(totalSec);
        }
    }

    /**
     * Decimal type hash
     * reference runtime implementation: #common/util/runtime_decimal.cpp
     * The implementation is so different between runtime c++ and java.
     * so far, Runtime c++ dependence on BkdrHash(the third library?)
     * TODO:
     * implement the same ToString method to output the same string to get the same hash val
     */
    private static class DecimalHasher implements OdpsHasher<BigDecimal> {
        @Override
        public int hash(BigDecimal val) {
            throw new RuntimeException("Not supported decimal type:" + val);
            /*
            if (val == null) {
                return 0;
            }
            return (int) (TypeHasher.getHasher("String").hash(val.toString()));
            */
        }
    }
}
