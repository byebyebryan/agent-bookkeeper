//! Small, dependency-free measurement helpers for controlled V1.5 runs.
//!
//! The values describe one local process invocation; they are evidence for a
//! deployment envelope, not a portable benchmark score.

use std::io;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::catalog::Catalog;
use crate::source_fs::{
    LayoutPlugin, ReconcileReport, Reconciler, RevisionHasher, ScrubReport, SourceError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessResources {
    pub user_cpu: Option<Duration>,
    pub system_cpu: Option<Duration>,
    /// Process lifetime high-water resident memory, where the host exposes it.
    /// It is not a per-run allocation delta.
    pub process_max_resident_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceDelta {
    pub user_cpu: Option<Duration>,
    pub system_cpu: Option<Duration>,
    pub process_max_resident_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct ReconcileMeasurement {
    pub report: ReconcileReport,
    pub elapsed: Duration,
    pub resources: ResourceDelta,
    pub hash_throughput_bytes_per_second: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct ScrubMeasurement {
    pub report: ScrubReport,
    pub elapsed: Duration,
    pub resources: ResourceDelta,
    pub hash_throughput_bytes_per_second: Option<f64>,
}

pub fn measure_reconciliation<L, H>(
    reconciler: &mut Reconciler<L, H>,
    catalog: &mut Catalog,
) -> Result<ReconcileMeasurement, MeasurementError>
where
    L: LayoutPlugin,
    H: RevisionHasher,
{
    let before = current_process_resources()?;
    let started = Instant::now();
    let report = reconciler.scan(catalog)?;
    let elapsed = started.elapsed();
    let after = current_process_resources()?;
    Ok(ReconcileMeasurement {
        hash_throughput_bytes_per_second: throughput(report.bytes_hashed, elapsed),
        report,
        elapsed,
        resources: resource_delta(before, after),
    })
}

pub fn measure_scrub<L, H>(
    reconciler: &mut Reconciler<L, H>,
    catalog: &mut Catalog,
    byte_budget: u64,
) -> Result<ScrubMeasurement, MeasurementError>
where
    L: LayoutPlugin,
    H: RevisionHasher,
{
    let before = current_process_resources()?;
    let started = Instant::now();
    let report = reconciler.scrub(catalog, byte_budget)?;
    let elapsed = started.elapsed();
    let after = current_process_resources()?;
    Ok(ScrubMeasurement {
        hash_throughput_bytes_per_second: throughput(report.bytes_hashed, elapsed),
        report,
        elapsed,
        resources: resource_delta(before, after),
    })
}

pub fn current_process_resources() -> Result<ProcessResources, MeasurementError> {
    current_process_resources_platform()
}

fn resource_delta(before: ProcessResources, after: ProcessResources) -> ResourceDelta {
    ResourceDelta {
        user_cpu: duration_delta(before.user_cpu, after.user_cpu),
        system_cpu: duration_delta(before.system_cpu, after.system_cpu),
        process_max_resident_bytes: after.process_max_resident_bytes,
    }
}

fn duration_delta(before: Option<Duration>, after: Option<Duration>) -> Option<Duration> {
    after
        .zip(before)
        .map(|(after, before)| after.saturating_sub(before))
}

fn throughput(bytes: u64, elapsed: Duration) -> Option<f64> {
    if bytes == 0 || elapsed.is_zero() {
        return None;
    }
    Some(bytes as f64 / elapsed.as_secs_f64())
}

#[cfg(unix)]
fn current_process_resources_platform() -> Result<ProcessResources, MeasurementError> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(MeasurementError::Resource(io::Error::last_os_error()));
    }
    let usage = unsafe { usage.assume_init() };
    let max_resident = u64::try_from(usage.ru_maxrss).ok();
    Ok(ProcessResources {
        user_cpu: timeval_duration(usage.ru_utime),
        system_cpu: timeval_duration(usage.ru_stime),
        #[cfg(target_os = "macos")]
        process_max_resident_bytes: max_resident,
        #[cfg(not(target_os = "macos"))]
        process_max_resident_bytes: max_resident.and_then(|value| value.checked_mul(1024)),
    })
}

#[cfg(unix)]
fn timeval_duration(value: libc::timeval) -> Option<Duration> {
    let seconds = u64::try_from(value.tv_sec).ok()?;
    let microseconds = u32::try_from(value.tv_usec).ok()?;
    if microseconds >= 1_000_000 {
        return None;
    }
    Some(Duration::new(seconds, microseconds * 1_000))
}

#[cfg(not(unix))]
fn current_process_resources_platform() -> Result<ProcessResources, MeasurementError> {
    Ok(ProcessResources {
        user_cpu: None,
        system_cpu: None,
        process_max_resident_bytes: None,
    })
}

#[derive(Debug, Error)]
pub enum MeasurementError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("could not inspect process resources: {0}")]
    Resource(#[source] io::Error),
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::Path;

    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{current_process_resources, measure_reconciliation, measure_scrub};
    use crate::ProducerId;
    use crate::catalog::Catalog;
    use crate::source_fs::{
        CodexRolloutLayout, DeletionMode, Reconciler, RootGuard, SourceConfig, SourceRoot,
        StabilityPolicy,
    };

    fn session_file(root: &Path, id: Uuid) {
        let sessions = root.join("sessions");
        fs::create_dir_all(&sessions).unwrap();
        let path = sessions.join(format!("rollout-2026-08-01T00-00-00-{id}.jsonl"));
        let mut file = File::create(path).unwrap();
        writeln!(
            file,
            "{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{id}\"}}}}"
        )
        .unwrap();
        file.write_all(b"measurement\n").unwrap();
    }

    #[test]
    fn measurements_record_hash_bytes_and_resource_snapshot() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        fs::write(root.join(".guard"), b"measurement root\n").unwrap();
        session_file(root, Uuid::new_v4());
        let guard = RootGuard::from_marker_bytes(".guard", b"measurement root\n").unwrap();
        let source_root = SourceRoot::new("active", root, Some(guard)).unwrap();
        let config = SourceConfig::new(
            "measurement",
            ProducerId::new(),
            vec![source_root],
            DeletionMode::Disabled,
            StabilityPolicy::new(2).unwrap(),
        )
        .unwrap();
        let mut reconciler = Reconciler::new(config, CodexRolloutLayout);
        let mut catalog = Catalog::open_in_memory().unwrap();

        let first = measure_reconciliation(&mut reconciler, &mut catalog).unwrap();
        let second = measure_reconciliation(&mut reconciler, &mut catalog).unwrap();
        let scrub = measure_scrub(&mut reconciler, &mut catalog, 0).unwrap();

        assert_eq!(first.report.bytes_hashed, 0);
        assert!(second.report.bytes_hashed > 0);
        assert!(second.hash_throughput_bytes_per_second.is_some());
        assert!(scrub.report.bytes_hashed > 0);
        assert!(current_process_resources().is_ok());
    }
}
