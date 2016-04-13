package com.aliyun.odps.fs;

import com.aliyun.odps.volume.VolumeFSUtil;
import com.aliyun.odps.volume.protocol.VolumeFSConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

/**
 * Created by tianli on 15/11/17.
 */
public class VolumeFs extends AbstractFileSystem {

  public VolumeFs(URI uri, String supportedScheme, boolean authorityNeeded, int defaultPort) throws URISyntaxException {
    super(uri, supportedScheme, authorityNeeded, defaultPort);
  }

  private VolumeFileSystem fs;

  public VolumeFs(final URI theUri, final Configuration conf) throws URISyntaxException, IOException {
    super(theUri, VolumeFileSystemConfigKeys.VOLUME_URI_SCHEME, false, -1);
    if (!theUri.getScheme().equalsIgnoreCase(VolumeFileSystemConfigKeys.VOLUME_URI_SCHEME)) {
      throw new IllegalArgumentException("Passed URI's scheme is not for ODPS Volume");
    }
    fs = new VolumeFileSystem();
    fs.initialize(theUri, conf);
  }

  @Override
  public int getUriDefaultPort() {
    return -1;
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return null;
  }

  @Override
  public FSDataOutputStream createInternal(Path f, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, UnresolvedLinkException, IOException {
    return fs.create(f, null, flag.contains(CreateFlag.OVERWRITE), bufferSize, replication, blockSize, progress);
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, UnresolvedLinkException, IOException {
    fs.mkdirs(dir, permission);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return fs.delete(f, recursive);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return fs.open(f, bufferSize);
  }

  @Override
  public boolean setReplication(Path f, short replication) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return fs.setReplication(f, replication);
  }

  @Override
  public void renameInternal(Path src, Path dst) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnresolvedLinkException, IOException {
    fs.rename(src, dst);
  }

  @Override
  public void setPermission(Path f, FsPermission permission) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

  }

  @Override
  public void setOwner(Path f, String username, String groupname) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {

  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return null;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return fs.getFileStatus(f);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return new BlockLocation[0];
  }

  @Override
  public FsStatus getFsStatus() throws AccessControlException, FileNotFoundException, IOException {
    return null;
  }

  @Override
  public FileStatus[] listStatus(Path f) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return fs.listStatus(f);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) throws AccessControlException, IOException {

  }
}
