package com.jahop.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * ByteUtils test class
 * Created by Pavel on 8/20/2016.
 */
public class ByteUtilsTest {
    private static final Logger log = LogManager.getLogger(ByteUtilsTest.class);
    @Test
    public void writeAndRead2LowerBytes() throws Exception {
        final int number = new Random(System.nanoTime()).nextInt();
        log.debug("Number: " + number);
        log.debug("Expected: " + (number & 0xffff));
        final ByteBuffer bb = ByteBuffer.allocate(2);
        ByteUtils.write2LowerBytes(bb, number);
        bb.flip();
        final int candidate = ByteUtils.read2LowerBytes(bb);
        Assert.assertEquals(number & 0xffff, candidate);
        log.debug(Integer.toBinaryString(candidate));
    }
}