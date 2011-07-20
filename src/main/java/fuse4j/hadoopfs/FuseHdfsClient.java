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

import fuse.Errno;
import fuse.Filesystem3;
import fuse.FuseDirFiller;
import fuse.FuseException;
import fuse.FuseGetattrSetter;
import fuse.FuseMount;
import fuse.FuseOpenSetter;
import fuse.FuseSizeSetter;
import fuse.FuseStatfs;
import fuse.FuseStatfsSetter;
import fuse.LifecycleSupport;
import fuse.XattrLister;
import fuse.XattrSupport;
import fuse.util.FuseArgumentParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.nativeio.NativeIO;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;

public class FuseHdfsClient implements Filesystem3, XattrSupport, LifecycleSupport {

    private static final Log log = LogFactory.getLog(FuseHdfsClient.class);

    private static final int BLOCK_SIZE = 512;
    private static final int NAME_LENGTH = 1024;

    private HdfsClient hdfs = null;

    private Thread ctxtMapCleanerThread = null;

    public FuseHdfsClient() {
        this(new String[]{});
    }

    public FuseHdfsClient(String [] args) {
        this(new FuseArgumentParser(args));
    }

    public FuseHdfsClient(FuseArgumentParser args) {
        hdfs = HdfsClientFactory.create(args);

        /*hdfsFileCtxtMap = new HashMap<String, HdfsFileContext>();
        ctxtMapCleanerThread = new Thread(this);
        ctxtMapCleanerThread.start();
        log.info("created");*/
    }

    public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        log.info("getattr(): " + path + "\n");
        HdfsFileAttr s = hdfs.getFileInfo(path);

        if(s == null) {
            return Errno.ENOENT;
        }

        long size;
        long blocks;
        if(s.directory) {
            size = 512;
            blocks = 2;
        } else {
            size = s.size;
            blocks = (s.size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        }

        getattrSetter.set(
            s.inode, s.getMode(), s.numberOfLinks,
            s.uid, s.gid, 0,
            size, blocks,
            s.accessTime, s.modifiedTime, s.createTime
        );

        return 0;

    }


    public int readlink(String path, CharBuffer link) throws FuseException {
        // Not Supported
        log.info("readlink(): " + path + "\n");
        return Errno.ENOENT;
    }

    public int getdir(String path, FuseDirFiller filler) throws FuseException {

        log.info("getdir(): " + path + "\n");

        HdfsDirEntry[] entries = hdfs.listPaths(path);

        if(entries == null) {
            return FuseException.ENOTDIR;
        }

        for(HdfsDirEntry entry : entries) {
            filler.add(entry.name, entry.hashCode(), entry.getMode());
        }

        return 0;
    }

    /**
     * mknod()
     */
    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod(): " + path + " mode: " + mode + "\n");
        return hdfs.mknod(path) ? 0 : FuseException.EREMOTEIO ;
        //if (hdfs.open(path, NativeIO.O_WRONLY | NativeIO.O_CREAT) != null)
        //  return 0;
        //return FuseException.EREMOTEIO;
/**
        // do a quick check to see if the file already exists
        HdfsFileContext ctxt = pinFileContext(path);

        if(ctxt != null) {
            unpinFileContext(path);
            return FuseException.EPERM;
        }

        // create the file
        Object hdfsFile = hdfs.createForWrite(path);

        //
        // track this newly opened file, for writing.
        //

        //
        // there will be no 'release' for this mknod, therefore we must not
        // pin the file, it will still live in the tree, but will have a
        // '0' pin-count.
        // TODO: eventually have a worker-thread that looks at all
        //       '0' pin-count objects, ensures that they were opened for
        //       write and releases() them if no writes have happened.
        //       (this hack is to support HDFS's write-once semantics).
        //

        if(!addFileContext(path, new HdfsFileContext(hdfsFile, true), false)) {
            // if we raced with another thread, then close off the open file
            // and fail this open().
            hdfs.close(hdfsFile);

            // TODO: don't fail this open() when racing with another
            //       thread, instead just use the
            //       already inserted 'context'...?
            return FuseException.EACCES;
        }
        return 0;

*/
    }

    public int mkdir(String path, int mode) throws FuseException {
        log.info("mkdir(): " + path + " mode: " + mode + "\n");

        boolean status = hdfs.mkdir(path);

        if(!status) {
            return FuseException.EREMOTEIO;
        }
        return 0;
    }

    public int unlink(String path) throws FuseException {
        log.info("unlink(): " + path + "\n");

        boolean status = hdfs.unlink(path);

        if(!status) {
            return FuseException.EREMOTEIO;
        }
        return 0;
    }

    public int rmdir(String path) throws FuseException {
        log.info("rmdir(): " + path + "\n");

        boolean status = hdfs.rmdir(path);

        if(!status) {
            return FuseException.EREMOTEIO;
        }

        return 0;
    }

    public int symlink(String from, String to) throws FuseException {
        //symlink not supported")
        return FuseException.ENOSYS;
    }

    public int rename(String from, String to) throws FuseException {
        log.info("rename(): " + from + " to: " + to + "\n");

        boolean status = hdfs.rename(from, to);

        if(!status) {
            return FuseException.EREMOTEIO;
        }
        return 0;
    }

    public int link(String from, String to) throws FuseException {
        log.info("link(): " + from + "\n");
        //link not supported")
        return FuseException.ENOSYS;
    }

    public int chmod(String path, int mode) throws FuseException {
        log.info("chmod(): " + path + "\n");
        // chmod not supported
        // but silently ignore the requests.
        return 0;
    }

    public int chown(String path, int uid, int gid) throws FuseException {
        log.info("chown(): " + path + "\n");
        throw new FuseException("chown not supported")
            .initErrno(FuseException.ENOSYS);
    }

    public int truncate(String path, long size) throws FuseException {
        log.info("truncate(): " + path + " size: "+ size);
        if (size!=0){
          /* @see HDFS-860 */
          return 0;
        }

        /* the requested size == 0, equivalent to delete & recreate file */
        boolean status = hdfs.unlink(path);
        if(!status) {
          return FuseException.EREMOTEIO;
        }
        //Create, open and close a file to set the emulate the fuse flow when creating a new file
        mknod(path, 0, 0);
        Object fh = hdfs.open(path, NativeIO.O_WRONLY | NativeIO.O_CREAT);
        if (fh == null) {
          return FuseException.EREMOTEIO;
        }
        if ( ! hdfs.close(fh) ){
          return FuseException.EREMOTEIO;
        }

        return 0;
    }

    public int utime(String path, int atime, int mtime) throws FuseException {
        log.info("utime(): " + path + "atime:" + atime + "mtime:"+ mtime + "\n" );
        return hdfs.utime(path, atime, mtime) ? 0 : FuseException.EACCES;
    }

    public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
      log.info("statfs(): \n");
      FuseStatfs status = hdfs.getStatus();
      if (status == null)
        return FuseException.EREMOTEIO;
      statfsSetter.set(status.blockSize, status.blocks, status.blocksFree, status.blocksAvail, status.files,
                       status.filesFree, status.namelen);
      return 0;
    }


    // if open returns a filehandle by calling FuseOpenSetter.setFh() method, it will be passed to every method that supports 'fh' argument
    /**
     * open()
     */
    @Override
    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        log.info("open(): " + path + " flags " + flags + "\n");
        Object fh = hdfs.open(path, flags);
        if (fh == null)
          return FuseException.EACCES;
        openSetter.setFh( fh );
        return 0;
    }

    // fh is filehandle passed from open
    /**
     * read()
     */
    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {

        //return Errno.EBADF;
        log.info("read(): " + path + " offset: " + offset + " len: "
            + buf.capacity() + "FH:" + fh + "\n");

        if (fh==null || ! hdfs.read(fh, buf, offset))
          return FuseException.EREMOTEIO;

        return 0;

    }

    // fh is filehandle passed from open,
    // isWritepage indicates that write was caused by a writepage
    /**
     * write()
     */
    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        log.info("write(): " + path + " offset: " + offset + " len: "
            + buf.capacity() + "\n");
        if (fh==null || ! hdfs.write(fh, buf, offset))
          return FuseException.EREMOTEIO;

        return 0;
    }

    // (called when last filehandle is closed), fh is filehandle passed from open
    public int release(String path, Object fh, int flags) throws FuseException {
        log.info("release(): " + path + " flags: " + flags + "\n");
        if (fh==null || ! hdfs.close(fh))
          return FuseException.EREMOTEIO;
        return 0;
    }

    public int flush(String path, Object fh) throws FuseException {
      log.info("flush(): " + path + " FH: " + fh + "\n");
      if (fh==null || ! hdfs.flush(fh))
        return FuseException.EREMOTEIO;
      return 0;
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        return 0;
    }


    public int getxattr(String path, String name, ByteBuffer dst, int position) throws FuseException, BufferOverflowException {
        return 0;
    }

    public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException {
        return Errno.ENOATTR;
    }

    public int listxattr(String path, XattrLister lister) throws FuseException {
        return 0;
    }

    public int removexattr(String path, String name) throws FuseException {
        return 0;
    }

    public int setxattr(String path, String name, ByteBuffer value, int flags, int position) throws FuseException {
        return 0;
    }

    // LifeCycleSupport
    public int init() {
        log.info("Initializing Filesystem");
        return 0;
    }

    public int destroy() {
        log.info("Destroying Filesystem");

        if(ctxtMapCleanerThread != null) {
            ctxtMapCleanerThread.interrupt();
        }

        try {
            System.exit(0);
        } catch (Exception e) {
        }

        return 0;
    }

    //
    // Java entry point
    public static void main(String[] args) {
        log.info("entering");

        try {
            FuseMount.mount(args, new FuseHdfsClient(args), log);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            log.info("exiting");
        }
    }

}
