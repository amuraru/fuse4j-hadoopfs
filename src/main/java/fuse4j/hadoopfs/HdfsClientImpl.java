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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * class HdfsClientReal
 */
class HdfsClientImpl implements HdfsClient {
    FileSystem dfs = null;
    private final UserCache userCache;
    private final Map<String, HdfsFileIoContext> newFiles;

    /**
     * constructor
     */
    HdfsClientImpl(UserCache userCache) throws IOException {
      this.userCache = userCache;
      this.newFiles = new ConcurrentHashMap<String, HdfsFileIoContext>();
      Configuration conf = new Configuration();
      this.dfs = FileSystem.get(conf);
    }

    @Override
    public FuseStatfs getStatus() {
      try {
        FsStatus status = dfs.getStatus();
        long cap = status.getCapacity();
        long bsize = dfs.getDefaultBlockSize();
        long used = status.getUsed();

        FuseStatfs statFS = new FuseStatfs();
        statFS.blockSize = (int) bsize;
        statFS.blocks = (int) (cap/bsize);
        statFS.blocksFree = (int) ((cap-used)/bsize);
        statFS.blocksAvail = (int) ((cap-used)/bsize);
        statFS.files = 1000;
        statFS.filesFree = 500;
        statFS.namelen = 1023;
        return statFS;
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }

    /**
     * getFileInfo()
     */
    @Override
    public HdfsFileAttr getFileInfo(String path) {

        try {
            FileStatus dfsStat = dfs.getFileStatus(new Path(path));

            final boolean directory = dfsStat.isDir();
            final int inode = 0;
            final int mode = dfsStat.getPermission().toShort();
            final int uid = userCache.getUid(dfsStat.getOwner());
            final int gid = 0;


            // TODO: per-file block-size can't be retrieved correctly,
            //       using default block size for now.
            final long size = dfsStat.getLen();
            final int blocks = (int) Math.ceil(((double) size) / dfs.getDefaultBlockSize());

            // modification/create-times are the same as access-time
            final int modificationTime = (int) (dfsStat.getModificationTime() / 1000);
            final int accessTime =  (int) (dfsStat.getAccessTime() / 1000);

            HdfsFileAttr hdfsFileAttr = new HdfsFileAttr(directory, inode, mode, uid, gid, 1);
            hdfsFileAttr.setSize(size, blocks);
            hdfsFileAttr.setTime(modificationTime, modificationTime, accessTime);

            // TODO Hack to set inode;
            hdfsFileAttr.inode = hdfsFileAttr.hashCode();

            return hdfsFileAttr;
        } catch(IOException ioe) {
            // fall through to failure
        }

        // failed
        return null;
    }

    /**
     * listPaths()
     */
    @Override
    public HdfsDirEntry[] listPaths(String path) {
        try {
            FileStatus[] dfsStatList = dfs.listStatus(new Path(path));
            HdfsDirEntry[] hdfsDirEntries = new HdfsDirEntry[dfsStatList.length + 2];

            // Add special directories.
            hdfsDirEntries[0] = new HdfsDirEntry(true, ".", 0777);
            hdfsDirEntries[1] = new HdfsDirEntry(true, "..", 0777);

            for(int i = 0; i < dfsStatList.length; i++) {
                hdfsDirEntries[i + 2] = newHdfsDirEntry(dfsStatList[i]);
            }

            return hdfsDirEntries;

        } catch(IOException ioe) {
            return null;
        }
    }

    private HdfsDirEntry newHdfsDirEntry(FileStatus fileStatus) {
        final boolean directory = fileStatus.isDir();
        final String name = fileStatus.getPath().getName();
        final FsPermission permission = fileStatus.getPermission();

        return new HdfsDirEntry(directory, name, permission.toShort());
    }

    @Override
    public Object open(String path, int flags){
      try {
        //based on fuse_impls_open in C fuse_dfs
        // 0x8000 is always passed in and hadoop doesn't like it, so killing it here
        // bugbug figure out what this flag is and report problem to Hadoop JIRA
        int hdfs_flags = (flags & 0x7FFF);
        System.out.println("HDFS CLIENT OPEN FILE:" + path +" mode:"+ Integer.toOctalString( hdfs_flags));

        //TODO: connect to DFS as calling user to enforce perms
        //see doConnectAsUser(dfs->nn_hostname, dfs->nn_port);

        if ((hdfs_flags & NativeIO.O_RDWR) == NativeIO.O_RDWR) {
          hdfs_flags ^= NativeIO.O_RDWR;
          try {
            FileStatus fileStatus = dfs.getFileStatus(new Path(path));
            if( this.newFiles.containsKey(path) ){
              // just previously created by "mknod" so open it in write-mode
              hdfs_flags |= NativeIO.O_WRONLY;
            } else {
              // File exists; open this as read only.
              hdfs_flags |= NativeIO.O_RDONLY;
            }
          } catch (IOException e) {
            // File does not exist (maybe?); interpret it as a O_WRONLY
            // If the actual error was something else, we'll get it again when
            // we try to open the file.
            hdfs_flags |= NativeIO.O_WRONLY;
          }
        }

        ///
        Path hPath = new Path(path);
        if ((hdfs_flags & NativeIO.O_WRONLY) == 0) {
          //READ
          System.out.println("HDFS OPEN file:"+path);
          return new HdfsFileIoContext(path, dfs.open(hPath));
        } else if ((hdfs_flags & NativeIO.O_APPEND) != 0) {
          //WRITE/APPEND
          System.out.println("HDFS APPEND file:"+path);
          return new HdfsFileIoContext(path, dfs.append(hPath));
        } else {
          //WRITE/CREATE
          System.out.println("HDFS CREATE file:"+path);
          System.out.println(this.newFiles.size());
          return this.newFiles.remove(path);
        }
      } catch (Exception e) {
        // fall through to failure
      }
      return null;
    }

    @Override
    public boolean mknod(String path){
      try {
        this.newFiles.put(path, new HdfsFileIoContext(path, dfs.create(new Path(path), true)));
        return true;
      } catch (IOException e) {
        return false;
      }
    }

    @Override
    public boolean close(Object hdfsFile) {
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;
        try {
            if(file.getIoStream() instanceof FSDataOutputStream) {
                FSDataOutputStream output = (FSDataOutputStream) file.getIoStream();
                output.close();
                return true;
            }

            if(file.getIoStream() instanceof FSDataInputStream) {
                FSDataInputStream output = (FSDataInputStream) file.getIoStream();
                output.close();
                return true;
            }
        } catch(IOException ioe) {
            // fall through to failure
        }

        return false;
    }

    /**
     * read()
     */
    @Override
    public boolean read(Object hdfsFile, ByteBuffer buf, long offset) {
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;

        if(!(file.getIoStream() instanceof FSDataInputStream)) {
            return false;
        }

        FSDataInputStream input = (FSDataInputStream) file.getIoStream();

        byte[] readBuf = new byte[buf.capacity()];

        int bytesRead = 0;
        try {
            bytesRead = input.read(offset, readBuf, 0, readBuf.length);
        } catch(IOException ioe) {
            return false;
        }

        // otherwise return how much we read
        // TODO: does this handle 0 bytes?
        if (bytesRead>0)
          buf.put(readBuf, 0, bytesRead);
        return true;
    }

    /**
     * flush()
     */
    @Override
    public boolean flush(Object hdfsFile) {
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;

        //Fuse calls "flush" also on R_ONLY files, so make sure we return true for these
        if(!(file.getIoStream() instanceof FSDataOutputStream)) {
            return true;
        }

        FSDataOutputStream output = (FSDataOutputStream) file.getIoStream();
        try {
          output.flush();
        } catch (IOException e) {
          return false;
        }
        return true;
    }
    /**
     * write()
     */
    @Override
    public boolean write(Object hdfsFile, ByteBuffer buf, long offset) {
        boolean status = false;
        HdfsFileIoContext file = (HdfsFileIoContext) hdfsFile;

        if(!(file.getIoStream() instanceof FSDataOutputStream)) {
            return false;
        }

        FSDataOutputStream output = (FSDataOutputStream) file.getIoStream();

        // get the data to write
        byte[] writeBuf = new byte[buf.capacity()];
        buf.get(writeBuf, 0, writeBuf.length);

        // lock this file so we can update the 'write-offset'
        synchronized(file) {
            // we will only allow contiguous writes
            //if(offset == file.getOffsetWritten()) {
                try {
                    output.write(writeBuf, 0, writeBuf.length);

                    // increase our offset
                    file.incOffsetWritten(writeBuf.length);

                    // return how much we read
                    // TODO: does this handle 0 bytes?
                    buf.position(writeBuf.length);

                    // if we are here, then everything is good
                    status = true;
                } catch(IOException ioe) {
                    // return failure
                    status = false;
                }
            //}
        }

        return status;
    }

    public boolean utime(String path, int atime, int mtime) {
      try {
        System.out.println("mtime:" + mtime);
        long latime = atime * 1000L;
        long lmtime = mtime * 1000L;
        dfs.setTimes(new Path(path), lmtime, latime);
        return true;
      } catch(IOException ioe) {
          // fall through to failure
      }
      return false;
    }
    /**
     * mkdir()
     */
    public boolean mkdir(String path) {
        try {
            return dfs.mkdirs(new Path(path));
        } catch(IOException ioe) {
            // fall through to failure
        }
        return false;
    }

    /**
     * unlink()
     */
    public boolean unlink(String filePath) {
        try {
            return dfs.delete(new Path(filePath));
        } catch(IOException ioe) {
            // fall through to failure
        }
        return false;
    }

    /**
     * rmdir()
     */
    public boolean rmdir(String dirPath) {
        return unlink(dirPath);
    }

    /**
     * rename()
     */
    public boolean rename(String src, String dst) {
      try {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        if (srcPath.equals(dstPath)) {
          //source and destination are the same path
          return false;
        }
        if (dfs.isFile(dstPath) && dfs.isFile(srcPath)) {
          //TODO: temporary fix to overwrite files
          //delete destination file if exists.
          //"HDFS-654"  fixes the problem allowing atomic rename when dst exists
          dfs.delete(dstPath);
        }
        return dfs.rename(srcPath, dstPath);
      } catch (IOException ioe) {
        // fall through to failure
        System.out.println(ioe);
      }
      return false;
    }

}

//
// class HdfsFileIoContext

//
class HdfsFileIoContext {
    private final Object ioStream;
    private long offsetWritten;
    private final String path;

    HdfsFileIoContext(String path, Object ioStream) {
        this.ioStream = ioStream;
        this.offsetWritten = 0;
        this.path = path;
    }

    public String getPath() {
      return path;
    }

    public long getOffsetWritten() {
      return offsetWritten;
    }

    public void incOffsetWritten(long incrementWritten) {
      this.offsetWritten += incrementWritten;
    }

    public Object getIoStream() {
      return ioStream;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return "HdfsFileIoContext [path=" + path + ", ioStream=" + ioStream + ", offsetWritten=" + offsetWritten + "]";
    }





}
