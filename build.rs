fn main() -> Result<(), Box<dyn std::error::Error>> {
  prost_build::compile_protos(&["proto/dropdb_rusty/v1/wal_entry.proto"],
                              &["proto/dropdb_rusty/v1"])?;

  Ok(())
}
