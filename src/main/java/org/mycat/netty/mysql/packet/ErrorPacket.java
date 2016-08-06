/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package org.mycat.netty.mysql.packet;

import io.netty.buffer.ByteBuf;
import org.mycat.netty.mysql.proto.ERR;
import org.mycat.netty.mysql.proto.Flags;
import org.mycat.netty.mysql.proto.Proto;
import org.mycat.netty.util.SysProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 * @author mycat
 */
public class ErrorPacket extends MySQLPacket {
	private static Logger logger = LoggerFactory.getLogger(ErrorPacket.class);

	public static final byte FIELD_COUNT = (byte) 0xff;
	private static final byte SQLSTATE_MARKER = (byte) '#';
	private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

	public byte fieldCount = FIELD_COUNT;
	public int errno;
	public byte mark = SQLSTATE_MARKER;
	public byte[] sqlState = DEFAULT_SQLSTATE;
	public byte[] message;

	public void read(BinaryPacket bin) {
		packetLength = bin.packetLength;
		packetId = bin.packetId;
		MySQLMessage mm = new MySQLMessage(bin.data);
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

	@Override
	public void write(ByteBuf buffer) {
		int size = calcPacketSize();
		BufUtil.writeUB3(buffer, size);
		buffer.writeByte(packetId);

		buffer.writeByte(fieldCount);
		BufUtil.writeUB2(buffer, errno);
		buffer.writeByte(mark);
		buffer.writeBytes(sqlState);
		if (message != null) {
			buffer.writeBytes(message);
		}
	}

	@Override
	public byte[] getPacket() {
		int size = calcPacketSize();
		byte[] packet = new byte[size+4];

		System.arraycopy(Proto.build_fixed_int(3, size), 0, packet, 0, 3);
		System.arraycopy(Proto.build_fixed_int(1, packetId), 0, packet, 3, 1);
		int offset = 4;

		packet[offset++] = fieldCount;
		System.arraycopy(Proto.build_fixed_int(2, errno), 0, packet, offset, 2);
		offset += 2;
		packet[offset++] = mark;
		System.arraycopy(sqlState, 0, packet, offset, sqlState.length);
		offset += sqlState.length;
		if (message != null) {
			System.arraycopy(message, 0, packet, offset, message.length);
		}
		logger.info("ErrPacket array : {} ", packet);
		logger.info("packet ln : " + packet.length + ", expected len: " + size);
		return packet;
	}

	@Override
	public int calcPacketSize() {
		int size = 9;// 1 + 2 + 1 + 5
		if (message != null) {
			size += message.length;
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Error Packet";
	}

}