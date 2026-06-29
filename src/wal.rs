/*!
  A write-ahead log (WAL for short, also known as a commit log) is an append-only auxiliary
  disk-resident structure used for crash and transaction recovery. The page cache allows buffering
  changes to page contents in memory. Until the cached contents are flushed back to disk, the only
  disk-resident copy preserving the operation history is stored in the WAL.

  The write-ahead log plays an important role in transaction processing. It is hard to overstate
  the importance of the WAL as it ensures that data makes it to the persistent storage and is
  available in case of a crash, as uncommitted data is replayed from the log and the pre-crash
  database state is fully restored.

  The write-ahead log is append-only and its written contents are immutable, so all writes to the
  log are sequential. Since the WAL is an immutable, append-only data structure, readers can safely
  access its contents up to the latest write threshold while the writer continues appending data to
  the log tail.

  The WAL consists of log records. Every record has a unique, monotonically increasing log sequence
  number (LSN). Usually, the LSN is represented by an internal counter or a timestamp. Since log
  records do not necessarily occupy an entire disk block, their contents are cached in the log
  buffer and are flushed on disk in a force operation. Forces happen as the log buffers fill up,
  and can be requested by the transaction manager or a page cache. All log records have to be
  flushed on disk in LSN order. Besides individual operation records, the WAL holds records
  indicating transaction completion. A transaction can’t be considered committed until the log is
  forced up to the LSN of its commit record.

  The WAL is usually coupled with a primary storage structure by the interface that allows trimming
  it whenever a checkpoint is reached. Logging is one of the most critical correctness aspects of
  the database, which is somewhat tricky to get right: even the slightest disagreements between log
  trimming and ensuring that the data has made it to the primary storage structure may cause data
  loss.

  Checkpoints are a way for a log to know that log records up to a certain mark are fully persisted
  and aren’t required anymore, which significantly reduces the amount of work required during the
  database startup. A process that forces all dirty pages to be flushed on disk is generally called
  a sync checkpoint, as it fully synchronizes the primary storage structure.

  NOTE : Flushing the entire contents on disk is rather impractical and would require pausing all
         running operations until the checkpoint is done, so most database systems implement fuzzy
         checkpoints. In this case, the last_checkpoint pointer stored in the log header contains
         the information about the last successful checkpoint. A fuzzy checkpoint begins with a
         special begin_checkpoint log record specifying its start, and ends with end_checkpoint log
         record, containing information about the dirty pages, and the contents of a transaction
         table. Until all the pages specified by this record are flushed, the checkpoint is
         considered to be incomplete. Pages are flushed asynchronously and, once this is done, the
         last_checkpoint record is updated with the LSN of the begin_checkpoint record and, in case
         of a crash, the recovery process will start from there
*/

use {
  crate::{
    error::{Error, Result},
    wal::proto::WalEntry
  },
  crc32fast::hash,
  parking_lot::{RwLock, RwLockWriteGuard},
  prost::Message,
  std::{
    cmp::{max, min},
    fs::{self, File, remove_file},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    mem,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Arc
  }
};

mod proto {
  //! We're using Protocol Buffers, so we don't need to hand write the code for binary
  //! serialization and deserialization of a WAL entry.

  include!(concat!(env!("OUT_DIR"), "/dropdb_rusty.v1.rs"));
}

const DEFAULT_DIR: &str = "~/.dropdb-rusty/wal";

const CHECKPOINT_FILE_NAME: &str = ".checkpoint";

const DEFAULT_MAX_SEGMENT_SIZE: SegmentSize = 16 * 1024; // = 16 KB.
const DEFAULT_MAX_SEGMENT_COUNT: usize = 1000;

const SEGMENT_NAME_PREFIX: &str = "segment-";

type SegmentID = u32;
type SegmentSize = u64;
type SegmentAppender = BufWriter<File>;

pub struct ThreadSafeWAL(Arc<RwLock<WAL>>);

pub struct WAL {
  /// The directory where segments are stored.
  dir: PathBuf,

  /// Path to the .checkpoint file, where information about the latest checkpoint is stored in
  /// this format : <segment ID>:<entry LSN>.
  /// When empty, it indicates that there are no checkpoints.
  checkpoint_file_path: PathBuf,

  /// Maximum byte size a segment can grow to.
  max_segment_size: SegmentSize,

  /// Maximum number of segments there can be.
  max_segment_count: usize,

  /// ID of the oldest segment.
  /// Lower the segment ID, older it is.
  oldest_segment_id: SegmentID,

  /// ID of the active segment, i.e., the segment to which entries will currently be appended.
  active_segment_id: SegmentID,

  /// Buffered writer to the active segment, used for appending entries.
  active_segment_appender: BufWriter<File>,

  /// LSN of the next entry to be appended.
  next_entry_lsn: u32
}

pub struct WALOptions {
  /// The directory where segments are stored.
  dir: PathBuf,

  /// Maximum byte size a segment can grow to.
  max_segment_size: SegmentSize,

  /// Maximum number of segments there can be.
  max_segment_count: usize
}

impl Default for WALOptions {
  fn default() -> Self {
    Self { dir: PathBuf::from(DEFAULT_DIR),

           max_segment_size:  DEFAULT_MAX_SEGMENT_SIZE,
           max_segment_count: DEFAULT_MAX_SEGMENT_COUNT }
  }
}

impl ThreadSafeWAL {
  pub fn new(options: WALOptions) -> Result<Self> {
    // Create the dir, if it doesn't already exist.
    fs::create_dir_all(&options.dir)?;

    // Determine the oldest and active segments.

    let mut segments_preexist: bool = false;

    let mut oldest_segment_id: SegmentID = 0;
    let mut active_segment_id: SegmentID = 0;

    for entry in fs::read_dir(&options.dir)? {
      let entry = entry?;

      if let Some(segment_id) =
        entry.path()
             .file_name()
             .and_then(|file_name| file_name.to_str())
             .and_then(|file_name| file_name.strip_prefix(SEGMENT_NAME_PREFIX))
             .and_then(|segment_id| segment_id.parse::<SegmentID>().ok())
      {
        segments_preexist = true;

        oldest_segment_id = min(segment_id, oldest_segment_id);
        active_segment_id = max(segment_id, active_segment_id);
      }
    }

    let active_segment_appender = get_segment_appender(&options.dir, active_segment_id)?;

    // Determine the next entry's LSN.
    let next_entry_lsn = match segments_preexist {
      // When no segments preexists, it's simply going to be 0.
      | false => 0,

      // Otherwise, we need to get the LSN of the last entry from the latest (or active) segment.
      // 1 + that LSN will then be the next entry's LSN.
      | true => {
        // Get the last entry in the active segment.
        let last_entry = get_last_entry_in_segment(&options.dir, active_segment_id)?;

        // Calculate the next entry's LSN.
        let next_entry_lsn = last_entry.lsn + 1;
        next_entry_lsn
      }
    };

    let wal = WAL { dir: options.dir.clone(),

                    checkpoint_file_path: options.dir.join(CHECKPOINT_FILE_NAME),

                    max_segment_size: options.max_segment_size,
                    max_segment_count: options.max_segment_count,

                    oldest_segment_id,

                    active_segment_id,
                    active_segment_appender,

                    next_entry_lsn };
    Ok(Self(Arc::new(RwLock::new(wal))))
  }

  /// First, rotates the WAL if necessary.
  /// Then, creates a new entry out of the given data, writing it to the active segment.
  pub fn write(&mut self, data: Vec<u8>) -> Result<()> {
    let mut wal = self.0.write();
    wal.write_entry(data, false)
  }

  /// First, rotates the WAL if necessary.
  /// Then, creates a new entry out of the given data, writing it to the active segment.
  /// And, that entry is also used as a checkpoint.
  pub fn checkpoint(&mut self, data: Vec<u8>) -> Result<()> {
    let mut wal = self.0.write();
    wal.write_entry(data, true)
  }
}

impl WAL {
  /// First, rotates the WAL if necessary.
  /// Then, creates a new entry out of the given data, writing it to the active segment.
  /// You can specify to use that entry as a checkpoint as well.
  fn write_entry(&mut self, data: Vec<u8>, is_checkpoint: bool) -> Result<()> {
    self.rotate_if_required()?;

    // Construct the WAL entry, and write it to the WAL segment.
    // NOTE : We aren't checking whether writing this entry will increase the active segment's
    //        size to more than the allowed maximum segment size. For now, it's okay.
    //        Maybe, I'll fix this later.

    let entry = WalEntry { lsn: self.next_entry_lsn,
                           is_checkpoint,

                           // TODO : When calculating the CRC, include the LSN along with the
                           //        data. That'll ensure that the ordering of entries is intact.
                           crc: hash(&data),
                           data };

    let mut buffer = Vec::with_capacity(4 + entry.encoded_len());

    buffer.extend_from_slice(&(entry.encoded_len() as u32).to_le_bytes());
    entry.encode(&mut buffer)?;

    self.active_segment_appender.write_all(&buffer)?;

    // When using this entry as a checkpoint,
    if is_checkpoint {
      // Ensure all changes in the active segment are persisted to the disk, if this is a
      // checkpoint.
      self.sync()?;

      // Update information about the latest checkpoint in the .checkpoint file.

      let latest_checkpoint_information = format!("{}:{}", self.active_segment_id, entry.lsn);

      let mut checkpoint_file_updater =
        File::options().write(true).truncate(true).open(&self.checkpoint_file_path)?;

      checkpoint_file_updater.write(latest_checkpoint_information.as_bytes())?;

      checkpoint_file_updater.sync_data()?;
    }

    // Update what will be the next entry's LSN.
    self.next_entry_lsn += 1;

    Ok(())
  }

  /// Syncs buffered entries to the disk.
  fn sync(&mut self) -> Result<()> {
    // Flush the buffered writer.
    self.active_segment_appender.flush()?;

    // Force the OS to persist the changes to disk, using fsync.
    self.active_segment_appender.get_ref().sync_all()?;

    // TODO : Reset periodic WAL sync timer.

    Ok(())
  }

  /// Calls .rotate( ), only if the currently active segment's size has become >= the allowed
  /// maximum segment size.
  fn rotate_if_required(&mut self) -> Result<()> {
    let active_segment_on_disk_size = self.active_segment_appender.get_ref().metadata()?.size();
    let active_segment_buffer_size = self.active_segment_appender.buffer().len() as SegmentSize;
    let active_segment_size = active_segment_on_disk_size + active_segment_buffer_size;
    if active_segment_size >= self.max_segment_size {
      self.rotate()?;
    }

    Ok(())
  }

  /// Ensures that all the changes in the current active segment is persisted to disk.
  ///
  /// Deletes the oldest segment if required, to comply with the allowed maximum number of
  /// segments.
  ///
  /// Then creates a new segment, setting as the current active segment.
  fn rotate(&mut self) -> Result<()> {
    // Ensure that all the changes in the current active segment is persisted to disk.
    self.sync()?;

    // Delete the oldest segment, if required.
    // Total number of segments should be <= self.max_segment_count.
    let segment_count = (self.active_segment_id - self.oldest_segment_id) as usize;
    if segment_count >= self.max_segment_count {
      let segment_path = self.dir.join(format!("{SEGMENT_NAME_PREFIX}{}", self.oldest_segment_id));

      remove_file(segment_path)?;

      self.oldest_segment_id += 1;
    }

    // Let's create the new active segment.
    self.active_segment_id += 1;
    self.active_segment_appender = get_segment_appender(&self.dir, self.active_segment_id)?;

    Ok(())
  }
}

impl Drop for WAL {
  fn drop(&mut self) {
    if let Err(error) = self.sync() {
      println!("Failed syncing WAL : {error}");
    }
  }
}

/// Returns a buffered writer wrapped file handler to the segment with the given ID, used to append
/// entries.
/// The segment gets created if it doesn't already exist.
fn get_segment_appender(dir: &Path, segment_id: SegmentID) -> io::Result<SegmentAppender> {
  let segment_name = format!("{SEGMENT_NAME_PREFIX}{segment_id}");
  let segment_path = dir.join(segment_name);

  let segment = File::options().create(true).read(true).append(true).open(segment_path)?;

  let segment_appender = BufWriter::new(segment);
  Ok(segment_appender)
}

/// Returns the last entry in the given segment.
fn get_last_entry_in_segment(wal_dir: &Path, segment_id: SegmentID) -> Result<proto::WalEntry> {
  let segment_path = wal_dir.join(format!("{SEGMENT_NAME_PREFIX}{segment_id}"));

  let mut segment_reader = File::options().read(true).open(segment_path)?;

  let segment_size = segment_reader.metadata()?.size();

  // Seek to the beginning of the file.
  segment_reader.seek(SeekFrom::Start(0))?;

  let mut last_entry_encoding_size = 0;

  loop {
    // Get the current entry encoding size.
    let mut current_entry_encoding_size: [u8; 4] = [0; 4];
    match segment_reader.read_exact(&mut current_entry_encoding_size) {
      // There are no entries left.
      // So, we now have the last entry.
      | Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,

      | Err(error) => return Err(error.into()),

      | _ => {}
    }
    let current_entry_encoding_size = u32::from_le_bytes(current_entry_encoding_size);

    segment_reader.seek(SeekFrom::Current(4))?;

    // Check if this is the last entry.
    // When yes, we can break out of the loop.

    let current_entry_ends_at =
      segment_reader.stream_position()? + (4 + current_entry_encoding_size) as u64;

    if current_entry_ends_at == segment_size {
      last_entry_encoding_size = current_entry_encoding_size;

      break;
    }

    // Otherwise, seek to the beginning of the next entry.
    segment_reader.seek(SeekFrom::Current(current_entry_encoding_size as i64))?;
  }

  // Read the last entry's encoding.
  let mut last_entry_encoding = Vec::with_capacity(last_entry_encoding_size as usize);
  segment_reader.read_exact(&mut last_entry_encoding)?;

  // Decode the last entry, and, verify it's data integrity.
  decode_entry(&last_entry_encoding)
}

/// Decodes the given entry encoding, and, verifies data integrity.
fn decode_entry(encoding: &[u8]) -> Result<WalEntry> {
  let entry = WalEntry::decode(encoding)?;

  // Verify data integrity.

  let current_crc = crc32fast::hash(encoding);
  if current_crc != entry.crc {
    return Err(Error::CorruptedWALEntry { lsn: entry.lsn });
  }

  Ok(entry)
}

pub struct WALReplayer<'wal_replayer> {
  /// Write lock on the thread safe WAL.
  thread_safe_wal: RwLockWriteGuard<'wal_replayer, WAL>,

  current_segment_id:     SegmentID,
  current_segment_reader: Option<File>
}

impl<'wal_replayer> WALReplayer<'wal_replayer> {
  /// Constructs an instance of the WAL replayer.
  /// We assume that a latest checkpoint already exists.
  pub fn new(thread_safe_wal: RwLockWriteGuard<'wal_replayer, WAL>) -> Result<Self> {
    // Read information about the latest checkpoint from the .checkpoint file.

    let checkpoint_file_path = thread_safe_wal.dir.join(CHECKPOINT_FILE_NAME);
    let latest_checkpoint_information = fs::read_to_string(checkpoint_file_path)?;

    let (current_segment_id, checkpoint_entry_lsn) =
      latest_checkpoint_information.trim().split_once(":").ok_or(Error::InvalidCheckpointFile)?;

    let current_segment_id =
      current_segment_id.parse::<SegmentID>().map_err(|_| Error::InvalidCheckpointFile)?;

    let checkpoint_entry_lsn =
      checkpoint_entry_lsn.parse::<u32>().map_err(|_| Error::InvalidCheckpointFile)?;

    // Create the segment reader.

    let current_segment_path =
      thread_safe_wal.dir.join(format!("{SEGMENT_NAME_PREFIX}{current_segment_id}"));

    let mut current_segment_reader = File::options().read(true).open(current_segment_path)?;

    current_segment_reader.seek(SeekFrom::Start(0))?;

    // And seek to the end of the entry which represents the latest checkpoint.
    loop {
      // Read the current entry encoding size.

      let mut current_entry_encoding_size: [u8; 4] = [0; 4];
      current_segment_reader.read_exact(&mut current_entry_encoding_size)?;
      let current_entry_encoding_size = u32::from_le_bytes(current_entry_encoding_size);

      current_segment_reader.seek(SeekFrom::Current(4))?;

      // Read and decode the current entry encoding.

      let mut current_entry_encoding = Vec::with_capacity(current_entry_encoding_size as usize);
      current_segment_reader.read_exact(&mut current_entry_encoding)?;

      let current_entry = decode_entry(&current_entry_encoding)?;

      current_segment_reader.seek(SeekFrom::Current(current_entry_encoding_size as i64))?;

      // That entry was the latest checkpoint.
      // So, we're done with seeking. And can start with replaying the WAL.
      if current_entry.lsn == checkpoint_entry_lsn {
        break;
      }
    }

    Ok(Self { thread_safe_wal,

              current_segment_id,
              current_segment_reader: Some(current_segment_reader) })
  }

  /// Get the next entry, if exists.
  pub fn try_next(&mut self) -> Result<Option<WalEntry>> {
    loop {
      // Try getting the current segment reader.
      let current_segment_reader = match self.current_segment_reader {
        // There are no more segments to read.
        // So, we don't need to do anything further.
        | None => return Ok(None),

        | Some(ref mut current_segment_reader) => current_segment_reader
      };

      // Get the seek position of the current segment reader.
      let seek_position = current_segment_reader.stream_position()?;

      // Check whether seek position is at the EOF : which means there are no more entries to be
      // read from the current segment, and we need to look if there is a next segment to read.

      let current_segment_size = current_segment_reader.metadata()?.size();

      if seek_position == current_segment_size {
        let next_segment_id = self.current_segment_id + 1;
        let next_segment_path =
          self.thread_safe_wal.dir.join(format!("{SEGMENT_NAME_PREFIX}{next_segment_id}"));

        let mut next_segment_reader = match File::options().read(true).open(next_segment_path) {
          // The next segment doesn't exist.
          // So, we don't need to do anything further.
          | Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),

          | result => result
        }?;

        // There is a next segment to be read.

        // Seek to it's beginning.
        next_segment_reader.seek(SeekFrom::Start(0))?;

        // And make it the current segment. We're going to process it in the next iteration.
        self.current_segment_id = next_segment_id;
        self.current_segment_reader = Some(next_segment_reader);

        continue;
      }

      // At this point, we're sure that there's an entry to be read in the current segment.

      // Read the current entry encoding size.

      let mut current_entry_encoding_size: [u8; 4] = [0; 4];
      current_segment_reader.read_exact(&mut current_entry_encoding_size)?;
      let current_entry_encoding_size = u32::from_le_bytes(current_entry_encoding_size);

      current_segment_reader.seek(SeekFrom::Current(4))?;

      // Read and decode the current entry encoding.

      let mut current_entry_encoding = Vec::with_capacity(current_entry_encoding_size as usize);
      current_segment_reader.read_exact(&mut current_entry_encoding)?;

      let current_entry = decode_entry(&current_entry_encoding)?;

      return Ok(Some(current_entry));
    }
  }
}
