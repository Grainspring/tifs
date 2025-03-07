use std::collections::BTreeMap;
use std::fmt::{self, Debug};
use std::future::Future;
use std::matches;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use anyhow::anyhow;
use async_std::task::sleep;
use async_trait::async_trait;
use bytes::Bytes;
use bytestring::ByteString;
use fuser::consts::FOPEN_DIRECT_IO;
use fuser::*;
use libc::{F_RDLCK, F_UNLCK, F_WRLCK, O_DIRECT, SEEK_CUR, SEEK_END, SEEK_SET};
use tikv_client::{Config, Key, TransactionClient, Value};
use tracing::{debug, error, info, instrument, trace, warn};

use super::dir::Directory;
use super::error::{FsError, Result};
use super::inode::Inode;
use super::key::{ScopedKey, ROOT_INODE};
use super::mode::make_mode;
use super::reply::get_time;
use super::reply::{Attr, Create, Data, Dir, DirItem, Entry, Lseek, Open, StatFs, Write};
use super::transaction::{LocalTxn, Txn};
use super::{async_fs::AsyncFileSystem, reply::Lock};
use crate::MountOption;

pub struct TiFs {
    pub pd_endpoints: Vec<String>,
    pub config: Config,
    pub client: TransactionClient,
    pub direct_io: bool,
    pub block_size: u64,
    entry_map: Arc<Mutex<BTreeMap<Key, Value>>>,
}

type BoxedFuture<'a, T> = Pin<Box<dyn 'a + Send + Future<Output = Result<T>>>>;

impl TiFs {
    pub const SCAN_LIMIT: u32 = 1 << 10;
    pub const DEFAULT_BLOCK_SIZE: u64 = 1 << 16;
    pub const MAX_NAME_LEN: u32 = 1 << 8;

    #[instrument]
    pub async fn construct<S>(
        pd_endpoints: Vec<S>,
        cfg: Config,
        options: Vec<MountOption>,
    ) -> anyhow::Result<Self>
    where
        S: Clone + Debug + Into<String>,
    {
        let client = TransactionClient::new_with_config(pd_endpoints.clone(), cfg.clone())
            .await
            .map_err(|err| anyhow!("{}", err))?;
        info!("connected to pd endpoints: {:?}", pd_endpoints);
        Ok(TiFs {
            client,
            pd_endpoints: pd_endpoints.clone().into_iter().map(Into::into).collect(),
            config: cfg,
            direct_io: options
                .iter()
                .find(|option| matches!(option, MountOption::DirectIO))
                .is_some(),
            block_size: options
                .iter()
                .find_map(|option| {
                    if let MountOption::BlkSize(size) = option {
                        Some(size << 10)
                    } else {
                        None
                    }
                })
                .unwrap_or(Self::DEFAULT_BLOCK_SIZE),
            entry_map: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    async fn process_txn<F, T>(&self, txn: &mut Txn, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, &'a mut Txn) -> BoxedFuture<'a, T>,
    {
        match f(self, txn).await {
            Ok(v) => {
                txn.commit().await?;
                trace!("transaction committed");
                Ok(v)
            }
            Err(e) => {
                txn.rollback().await?;
                debug!("transaction rollbacked");
                Err(e)
            }
        }
    }

    async fn with_optimistic<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, &'a mut Txn) -> BoxedFuture<'a, T>,
    {
        let mut txn = Txn::begin_optimistic(&self.client, self.block_size).await?;
        self.process_txn(&mut txn, f).await
    }

    async fn spin<F, T>(&self, delay: Option<Duration>, mut f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, &'a mut Txn) -> BoxedFuture<'a, T>,
    {
        loop {
            match self.with_optimistic(&mut f).await {
                Ok(v) => break Ok(v),
                Err(FsError::KeyError(err)) => {
                    trace!("spin because of a key error({})", err);
                    if let Some(time) = delay {
                        sleep(time).await;
                    }
                }
                Err(err) => break Err(err),
            }
        }
    }

    async fn process_txn_local<F, T>(&self, txn: &mut LocalTxn, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, &'a mut LocalTxn) -> BoxedFuture<'a, T>,
    {
        match f(self, txn).await {
            Ok(v) => {
                // txn.commit().await?;
                trace!("transaction committed");
                Ok(v)
            }
            Err(e) => {
                // txn.rollback().await?;
                debug!("transaction rollbacked");
                Err(e)
            }
        }
    }

    async fn with_optimistic_local<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnOnce(&'a TiFs, &'a mut LocalTxn) -> BoxedFuture<'a, T>,
    {
        let mut local_txn =
            LocalTxn::begin_optimistic(self.entry_map.clone(), self.block_size).await?;
        self.process_txn_local(&mut local_txn, f).await
    }

    async fn spin_local<F, T>(&self, delay: Option<Duration>, mut f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, &'a mut LocalTxn) -> BoxedFuture<'a, T>,
    {
        loop {
            match self.with_optimistic_local(&mut f).await {
                Ok(v) => break Ok(v),
                Err(FsError::KeyError(err)) => {
                    trace!("spin because of a key error({})", err);
                    if let Some(time) = delay {
                        sleep(time).await;
                    }
                }
                Err(err) => break Err(err),
            }
        }
    }

    #[cfg(feature = "kv_store")]
    async fn spin_no_delay_local<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, &'a mut Txn) -> BoxedFuture<'a, T>,
    {
        self.spin(None, f).await
    }

    #[cfg(feature = "mem_store")]
    async fn spin_no_delay_local<F, T>(&self, f: F) -> Result<T>
    where
        T: 'static + Send,
        F: for<'a> FnMut(&'a TiFs, &'a mut LocalTxn) -> BoxedFuture<'a, T>,
    {
        self.spin_local(None, f).await
    }

    async fn read_dir(&self, ino: u64) -> Result<Directory> {
        self.spin_no_delay_local(move |_, txn| Box::pin(txn.read_dir(ino)))
            .await
    }

    async fn read_inode(&self, ino: u64) -> Result<FileAttr> {
        let ino = self
            .spin_no_delay_local(move |_, txn| Box::pin(txn.read_inode(ino)))
            .await?;
        Ok(ino.file_attr)
    }

    async fn setlkw(&self, ino: u64, lock_owner: u64, typ: i32) -> Result<bool> {
        loop {
            let res = self
                .spin_no_delay_local(move |_, txn| {
                    Box::pin(async move {
                        let mut inode = txn.read_inode(ino).await?;
                        match typ {
                            F_WRLCK => {
                                if inode.lock_state.owner_set.len() > 1 {
                                    return Ok(false);
                                }
                                if inode.lock_state.owner_set.is_empty() {
                                    inode.lock_state.lk_type = F_WRLCK;
                                    inode.lock_state.owner_set.insert(lock_owner);
                                    txn.save_inode(&inode).await?;
                                    return Ok(true);
                                }
                                if inode.lock_state.owner_set.get(&lock_owner) == Some(&lock_owner)
                                {
                                    inode.lock_state.lk_type = F_WRLCK;
                                    txn.save_inode(&inode).await?;
                                    return Ok(true);
                                }
                                Err(FsError::InvalidLock)
                            }
                            F_RDLCK => {
                                if inode.lock_state.lk_type == F_WRLCK {
                                    return Ok(false);
                                } else {
                                    inode.lock_state.lk_type = F_RDLCK;
                                    inode.lock_state.owner_set.insert(lock_owner);
                                    txn.save_inode(&inode).await?;
                                    return Ok(true);
                                }
                            }
                            _ => return Err(FsError::InvalidLock),
                        }
                    })
                })
                .await?;
            if res {
                break;
            }
        }

        Ok(true)
    }

    fn check_file_name(name: &str) -> Result<()> {
        if name.len() <= Self::MAX_NAME_LEN as usize {
            Ok(())
        } else {
            Err(FsError::NameTooLong {
                file: name.to_string(),
            })
        }
    }
}

impl Debug for TiFs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("tifs({:?})", self.pd_endpoints))
    }
}

#[async_trait]
impl AsyncFileSystem for TiFs {
    #[tracing::instrument]
    async fn init(&self, gid: u32, uid: u32, config: &mut KernelConfig) -> Result<()> {
        // config
        //     .add_capabilities(fuser::consts::FUSE_POSIX_LOCKS)
        //     .expect("kernel config failed to add cap_fuse FUSE_POSIX_LOCKS");
        config
            .add_capabilities(fuser::consts::FUSE_FLOCK_LOCKS)
            .expect("kernel config failed to add cap_fuse FUSE_CAP_FLOCK_LOCKS");

        self.spin_no_delay_local(move |fs, txn| {
            Box::pin(async move {
                info!("initializing tifs on {:?} ...", &fs.pd_endpoints);
                if let Some(meta) = txn.read_meta().await? {
                    if meta.block_size != txn.block_size() {
                        let err = FsError::block_size_conflict(meta.block_size, txn.block_size());
                        error!("{}", err);
                        return Err(err);
                    }
                }

                let root_inode = txn.read_inode(ROOT_INODE).await;
                if let Err(FsError::InodeNotFound { inode: _ }) = root_inode {
                    let attr = txn
                        .mkdir(
                            0,
                            Default::default(),
                            make_mode(FileType::Directory, 0o777),
                            gid,
                            uid,
                        )
                        .await?;
                    debug!("make root directory {:?}", &attr);
                    Ok(())
                } else {
                    root_inode.map(|_| ())
                }
            })
        })
        .await
    }

    #[tracing::instrument]
    async fn lookup(&self, parent: u64, name: ByteString) -> Result<Entry> {
        Self::check_file_name(&name)?;
        self.spin_no_delay_local(move |_, txn| {
            let name = name.clone();
            Box::pin(async move {
                let ino = txn.lookup(parent, name).await?;
                Ok(Entry::new(txn.read_inode(ino).await?.into(), 0))
            })
        })
        .await
    }

    #[tracing::instrument]
    async fn getattr(&self, ino: u64) -> Result<Attr> {
        Ok(Attr::new(self.read_inode(ino).await?))
    }

    #[tracing::instrument]
    async fn setattr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<Attr> {
        self.spin_no_delay_local(move |_, txn| {
            Box::pin(async move {
                // TODO: how to deal with fh, chgtime, bkuptime?
                let mut attr = txn.read_inode(ino).await?;
                attr.perm = match mode {
                    Some(m) => m as _,
                    None => attr.perm,
                };
                attr.uid = uid.unwrap_or(attr.uid);
                attr.gid = gid.unwrap_or(attr.gid);
                attr.set_size(size.unwrap_or(attr.size), txn.block_size());
                attr.atime = match atime {
                    None => attr.atime,
                    Some(TimeOrNow::SpecificTime(t)) => t,
                    Some(TimeOrNow::Now) => SystemTime::now(),
                };
                attr.mtime = match mtime {
                    Some(TimeOrNow::SpecificTime(t)) => t,
                    Some(TimeOrNow::Now) | None => SystemTime::now(),
                };
                attr.ctime = ctime.unwrap_or(SystemTime::now());
                attr.crtime = crtime.unwrap_or(attr.crtime);
                attr.flags = flags.unwrap_or(attr.flags);
                txn.save_inode(&attr).await?;
                Ok(Attr {
                    time: get_time(),
                    attr: attr.into(),
                })
            })
        })
        .await
    }

    #[tracing::instrument]
    async fn readdir(&self, ino: u64, _fh: u64, mut offset: i64) -> Result<Dir> {
        let mut dir = Dir::offset(offset as usize);

        if offset == 0 {
            dir.push(DirItem {
                ino: ROOT_INODE,
                name: "..".to_string(),
                typ: FileType::Directory,
            });
        }

        if offset <= 1 {
            dir.push(DirItem {
                ino,
                name: ".".to_string(),
                typ: FileType::Directory,
            });
        }

        offset -= 2.min(offset);

        let directory = self.read_dir(ino).await?;
        for (item) in directory.into_iter().skip(offset as usize) {
            dir.push(item)
        }
        debug!("read directory {:?}", &dir);
        Ok(dir)
    }

    #[tracing::instrument]
    async fn open(&self, ino: u64, flags: i32) -> Result<Open> {
        // TODO: deal with flags
        let fh = self
            .spin_no_delay_local(move |_, txn| Box::pin(txn.open(ino)))
            .await?;

        let mut open_flags = 0;
        if self.direct_io || flags | O_DIRECT != 0 {
            open_flags |= FOPEN_DIRECT_IO;
        }

        Ok(Open::new(fh, open_flags))
    }

    #[tracing::instrument]
    async fn read(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<Data> {
        let data = self
            .spin_no_delay_local(move |_, txn| Box::pin(txn.read(ino, fh, offset, size)))
            .await?;
        Ok(Data::new(data))
    }

    #[tracing::instrument(skip(data))]
    async fn write(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        data: Vec<u8>,
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<Write> {
        let data: Bytes = data.into();
        let len = self
            .spin_no_delay_local(move |_, txn| Box::pin(txn.write(ino, fh, offset, data.clone())))
            .await?;
        Ok(Write::new(len as u32))
    }

    /// Create a directory.
    #[tracing::instrument]
    async fn mkdir(
        &self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        _umask: u32,
    ) -> Result<Entry> {
        Self::check_file_name(&name)?;
        let attr = self
            .spin_no_delay_local(move |_, txn| {
                Box::pin(txn.mkdir(parent, name.clone(), mode, gid, uid))
            })
            .await?;
        Ok(Entry::new(attr.into(), 0))
    }

    #[tracing::instrument]
    async fn rmdir(&self, parent: u64, raw_name: ByteString) -> Result<()> {
        Self::check_file_name(&raw_name)?;
        self.spin_no_delay_local(move |_, txn| Box::pin(txn.rmdir(parent, raw_name.clone())))
            .await
    }

    #[tracing::instrument]
    async fn mknod(
        &self,
        parent: u64,
        name: ByteString,
        mode: u32,
        gid: u32,
        uid: u32,
        _umask: u32,
        rdev: u32,
    ) -> Result<Entry> {
        Self::check_file_name(&name)?;
        let attr = self
            .spin_no_delay_local(move |_, txn| {
                Box::pin(txn.make_inode(parent, name.clone(), mode, gid, uid, rdev))
            })
            .await?;
        Ok(Entry::new(attr.into(), 0))
    }

    #[tracing::instrument]
    async fn access(&self, ino: u64, mask: i32) -> Result<()> {
        Ok(())
    }

    async fn create(
        &self,
        uid: u32,
        gid: u32,
        parent: u64,
        name: ByteString,
        mode: u32,
        umask: u32,
        flags: i32,
    ) -> Result<Create> {
        Self::check_file_name(&name)?;
        let entry = self.mknod(parent, name, mode, gid, uid, umask, 0).await?;
        let open = self.open(entry.stat.ino, flags).await?;
        Ok(Create::new(
            entry.stat,
            entry.generation,
            open.fh,
            open.flags,
        ))
    }

    async fn lseek(&self, ino: u64, fh: u64, offset: i64, whence: i32) -> Result<Lseek> {
        self.spin_no_delay_local(move |_, txn| {
            Box::pin(async move {
                let mut file_handler = txn.read_fh(ino, fh).await?;
                let inode = txn.read_inode(ino).await?;
                let target_cursor = match whence {
                    SEEK_SET => offset,
                    SEEK_CUR => file_handler.cursor as i64 + offset,
                    SEEK_END => inode.size as i64 + offset,
                    _ => return Err(FsError::UnknownWhence { whence }),
                };

                if target_cursor < 0 {
                    return Err(FsError::InvalidOffset {
                        ino: inode.ino,
                        offset: target_cursor,
                    });
                }

                file_handler.cursor = target_cursor as u64;
                txn.save_fh(ino, fh, &file_handler).await?;
                Ok(Lseek::new(target_cursor))
            })
        })
        .await
    }

    async fn release(
        &self,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
    ) -> Result<()> {
        self.spin_no_delay_local(move |_, txn| Box::pin(txn.close(ino, fh)))
            .await
    }

    /// Create a hard link.
    async fn link(&self, ino: u64, newparent: u64, newname: ByteString) -> Result<Entry> {
        Self::check_file_name(&newname)?;
        let inode = self
            .spin_no_delay_local(move |_, txn| Box::pin(txn.link(ino, newparent, newname.clone())))
            .await?;
        Ok(Entry::new(inode.into(), 0))
    }

    async fn unlink(&self, parent: u64, raw_name: ByteString) -> Result<()> {
        self.spin_no_delay_local(move |_, txn| Box::pin(txn.unlink(parent, raw_name.clone())))
            .await
    }

    async fn rename(
        &self,
        parent: u64,
        raw_name: ByteString,
        newparent: u64,
        new_raw_name: ByteString,
        _flags: u32,
    ) -> Result<()> {
        Self::check_file_name(&raw_name)?;
        Self::check_file_name(&new_raw_name)?;
        self.spin_no_delay_local(move |_, txn| {
            let name = raw_name.clone();
            let new_name = new_raw_name.clone();
            Box::pin(async move {
                let ino = txn.lookup(parent, name.clone()).await?;
                txn.link(ino, newparent, new_name).await?;
                txn.unlink(parent, name).await
            })
        })
        .await
    }

    #[tracing::instrument]
    async fn symlink(
        &self,
        gid: u32,
        uid: u32,
        parent: u64,
        name: ByteString,
        link: ByteString,
    ) -> Result<Entry> {
        Self::check_file_name(&name)?;
        self.spin_no_delay_local(move |_, txn| {
            let name = name.clone();
            let link = link.clone();
            Box::pin(async move {
                let mut attr = txn
                    .make_inode(
                        parent,
                        name,
                        make_mode(FileType::Symlink, 0o777),
                        gid,
                        uid,
                        0,
                    )
                    .await?;

                txn.write_link(&mut attr, link.into_bytes()).await?;
                Ok(Entry::new(attr.into(), 0))
            })
        })
        .await
    }

    async fn readlink(&self, ino: u64) -> Result<Data> {
        self.spin_local(None, move |_, txn| {
            Box::pin(async move { Ok(Data::new(txn.read_link(ino).await?)) })
        })
        .await
    }

    #[tracing::instrument]
    async fn fallocate(
        &self,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        _mode: i32,
    ) -> Result<()> {
        self.spin_no_delay_local(move |_, txn| {
            Box::pin(async move {
                let mut inode = txn.read_inode(ino).await?;
                txn.fallocate(&mut inode, offset, length).await
            })
        })
        .await?;
        Ok(())
    }

    // TODO: Find an api to calculate total and available space on tikv.
    #[cfg(feature = "kv_store")]
    async fn statfs(&self, _ino: u64) -> Result<StatFs> {
        let bsize = self.block_size as u32;
        let namelen = Self::MAX_NAME_LEN;
        let (ffree, blocks, files) = self
            .spin_no_delay_local(move |_, txn| {
                Box::pin(async move {
                    let next_inode = txn
                        .read_meta()
                        .await?
                        .map(|meta| meta.inode_next)
                        .unwrap_or(ROOT_INODE);
                    let (b, f) = txn
                        .scan(
                            ScopedKey::inode_range(ROOT_INODE..next_inode),
                            (next_inode - ROOT_INODE) as u32,
                        )
                        .await?
                        .map(|pair| Inode::deserialize(pair.value()))
                        .try_fold((0, 0), |(blocks, files), inode| {
                            Ok::<_, FsError>((blocks + inode?.blocks, files + 1))
                        })?;
                    Ok((std::u64::MAX - next_inode, b, f))
                })
            })
            .await?;
        Ok(StatFs::new(
            blocks,
            std::u64::MAX,
            std::u64::MAX,
            files,
            ffree,
            bsize,
            namelen,
            0,
        ))
    }

    #[cfg(feature = "mem_store")]
    #[tracing::instrument]
    async fn statfs(&self, _ino: u64) -> Result<StatFs> {
        let bsize = self.block_size as u32;
        let namelen = Self::MAX_NAME_LEN;

        let (ffree, blocks, files) = self
            .spin_no_delay_local(move |_, txn| {
                Box::pin(async move {
                    let next_inode = txn
                        .read_meta()
                        .await?
                        .map(|meta| meta.inode_next)
                        .unwrap_or(ROOT_INODE);
                    let local = txn.entry_map.lock().unwrap();
                    let range_data = local.range(ScopedKey::inode_range(ROOT_INODE..next_inode));
                    let (b, f) = range_data.map(|pair| Inode::deserialize(pair.1)).try_fold(
                        (0, 0),
                        |(blocks, files), inode| {
                            Ok::<_, FsError>((blocks + inode?.blocks, files + 1))
                        },
                    )?;
                    Ok((std::u64::MAX - next_inode, b, f))
                })
            })
            .await?;
        Ok(StatFs::new(
            blocks,
            std::u64::MAX,
            std::u64::MAX,
            files,
            ffree,
            bsize,
            namelen,
            0,
        ))
    }

    #[tracing::instrument]
    async fn setlk(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
        sleep: bool,
    ) -> Result<()> {
        let not_again = self.spin_no_delay_local(move |_, txn| {
            Box::pin(async move {
                let mut inode = txn.read_inode(ino).await?;
                warn!("setlk, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                if inode.file_attr.kind == FileType::Directory {
                    return Err(FsError::InvalidLock);
                }
                match typ {
                    F_RDLCK => {
                        if inode.lock_state.lk_type == F_WRLCK {
                            if sleep {
                                warn!("setlk F_RDLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                return Ok(false)
                            }
                            return Err(FsError::InvalidLock);
                        }
                        inode.lock_state.owner_set.insert(lock_owner);
                        inode.lock_state.lk_type = F_RDLCK;
                        txn.save_inode(&inode).await?;
                        warn!("setlk F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                        Ok(true)
                    }
                    F_WRLCK => match inode.lock_state.lk_type {
                        F_RDLCK => {
                            if inode.lock_state.owner_set.len() == 1
                                && inode.lock_state.owner_set.get(&lock_owner) == Some(&lock_owner)
                            {
                                inode.lock_state.lk_type = F_WRLCK;
                                txn.save_inode(&inode).await?;
                                warn!("setlk F_WRLCK on F_RDLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                return Ok(true);
                            }
                            if sleep {
                                warn!("setlk F_WRLCK on F_RDLCK sleep return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                return Ok(false)
                            }
                            return Err(FsError::InvalidLock);
                        },
                        F_UNLCK => {
                            inode.lock_state.owner_set.clear();
                            inode.lock_state.owner_set.insert(lock_owner);
                            inode.lock_state.lk_type = F_WRLCK;
                            warn!("setlk F_WRLCK on F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                            txn.save_inode(&inode).await?;
                            Ok(true)
                        },
                        F_WRLCK => {
                            if sleep {
                                warn!("setlk F_WRLCK on F_WRLCK return sleep, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                                return Ok(false)
                            }
                            return Err(FsError::InvalidLock);
                        },
                        _ => return Err(FsError::InvalidLock)
                    },
                    F_UNLCK => {
                        inode.lock_state.owner_set.remove(&lock_owner);
                        if inode.lock_state.owner_set.is_empty() {
                            inode.lock_state.lk_type = F_UNLCK;
                        }
                        txn.save_inode(&inode).await?;
                        warn!("setlk F_UNLCK return, inode:{:?}, pid:{:?}, typ para: {:?}, state type: {:?}, owner: {:?}, sleep: {:?},", inode, pid, typ, inode.lock_state.lk_type, lock_owner, sleep);
                        Ok(true)
                    }
                    _ => return Err(FsError::InvalidLock)
                }
            })
        })
        .await?;
        if !not_again {
            if self.setlkw(ino, lock_owner, typ).await? {
                return Ok(());
            }
            return Err(FsError::InvalidLock);
        }
        return Ok(());
    }

    #[tracing::instrument]
    async fn getlk(
        &self,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        start: u64,
        end: u64,
        typ: i32,
        pid: u32,
    ) -> Result<Lock> {
        // TODO: read only operation need not txn?
        self.spin_no_delay_local(move |_, txn| {
            Box::pin(async move {
                let inode = txn.read_inode(ino).await?;
                warn!("getlk, inode:{:?}, pid:{:?}", inode, pid);
                Ok(Lock::_new(0, 0, inode.lock_state.lk_type, 0))
            })
        })
        .await
    }
}
