package fuse4j.hadoopfs;
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

import fuse.FuseStatfs;

import java.nio.ByteBuffer;

/**
 * interface HdfsClient
 */
public interface HdfsClient {

    public FuseStatfs getStatus(int uid);

    public HdfsFileAttr getFileInfo(int uid, String path);

    public HdfsDirEntry[] listPaths(int uid, String path);

    /**
     * @param path
     * @return Object --> hdfsFile, that should be passed to close()
     */
    public Object open(int uid, String path, int flags);

    public boolean close(int uid, Object hdfsFile);

    public boolean read(int uid, Object hdfsFile, ByteBuffer buf, long offset);

    public boolean write(int uid, Object hdfsFile, ByteBuffer buf, long offset);

    public boolean flush(int uid, Object hdfsFile);

    public boolean utime(int uid, String path, int atime, int mtime);

    public boolean mknod(int uid, String path);

    public boolean mkdir(int uid, String path);

    public boolean unlink(int uid, String filePath);

    public boolean rmdir(int uid, String dirPath);

    public boolean rename(int uid, String src, String dst);


}