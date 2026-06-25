use serde::{Deserialize, Serialize};
#[cfg(feature = "metrics")]
pub use with_metrics::*;
#[cfg(all(feature = "profiler", not(feature = "metrics")))]
pub use with_profiler::*;
#[cfg(not(any(feature = "profiler", feature = "metrics")))]
pub use without_profiler::*;

use crate::{block::BlockStructure, network::Coord, scheduler::BlockId};

#[cfg(all(feature = "profiler", not(feature = "metrics")))]
mod bucket_profiler;

#[cfg(feature = "ssh")]
pub const TRACING_PREFIX: &str = "__renoir_TRACING_DATA__";

/// The available profiling metrics.
///
/// Calling one of those function will store the event inside the current profiler, if any. All of
/// them are no-op if the `profiler` feature is not enabled.
pub trait Profiler {
    /// Increase the number of received items in a block.
    fn items_in(&mut self, from: Coord, to: Coord, amount: usize);
    /// Increase the number of sent items from a block.
    fn items_out(&mut self, from: Coord, to: Coord, amount: usize);
    /// Increase the number of received bytes from the network to a block.
    fn net_bytes_in(&mut self, from: Coord, to: Coord, amount: usize);
    /// Increase the number of sent bytes from the network from a block.
    fn net_bytes_out(&mut self, from: Coord, to: Coord, amount: usize);
    /// Mark the end of an iteration.
    fn iteration_boundary(&mut self, leader_block_id: BlockId);
}

/// Tracing information of the current execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TracingData {
    pub structures: Vec<(Coord, BlockStructure)>,
    pub profilers: Vec<ProfilerResult>,
}

// impl Add for TracingData {
//     type Output = TracingData;

//     fn add(mut self, rhs: Self) -> Self::Output {
//         self += rhs;
//         self
//     }
// }

// impl AddAssign for TracingData {
//     fn add_assign(&mut self, mut rhs: Self) {
//         self.structures.append(&mut rhs.structures);
//         self.profilers.append(&mut rhs.profilers);
//     }
// }

pub fn log_trace(structures: Vec<(Coord, BlockStructure)>, profilers: Vec<ProfilerResult>) {
    if !cfg!(feature = "profiler") {
        return;
    }

    use std::io::Write as _;
    let data = TracingData {
        structures,
        profilers,
    };

    let mut stderr = std::io::stderr().lock();
    writeln!(
        stderr,
        "__renoir_TRACING_DATA__{}",
        serde_json::to_string(&data).unwrap()
    )
    .unwrap();
}

#[cfg(feature = "ssh")]
#[inline]
pub fn try_parse_trace(s: &str) -> Option<TracingData> {
    if let Some(s) = s.strip_prefix(TRACING_PREFIX) {
        match serde_json::from_str::<TracingData>(s) {
            Ok(trace) => Some(trace),
            Err(e) => {
                tracing::error!("Corrupted tracing data ({e}) `{s}`");
                None
            }
        }
    } else {
        None
    }
}

/// The implementation of the profiler when the `profiler` feature is disabled.
#[cfg(not(any(feature = "profiler", feature = "metrics")))]
mod without_profiler {
    use std::cell::UnsafeCell;

    use crate::network::Coord;
    use crate::profiler::*;

    /// The fake profiler for when the `profiler` feature is disabled.
    /// static PROFILER: UnsafeCell<NoOpProfiler> = UnsafeCell::new(NoOpProfiler);
    ///
    /// Fake profiler. This is used when the `profiler` feature is not enabled.
    ///
    /// This struct MUST NOT contain any field and must do absolutely nothing since it is accessed
    /// from a static reference.
    #[derive(Debug, Clone, Copy, Default)]
    pub struct NoOpProfiler;

    #[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
    pub struct ProfilerResult;

    thread_local! {
        static PROFILER: UnsafeCell<NoOpProfiler> = const { UnsafeCell::new(NoOpProfiler) };
    }

    impl Profiler for NoOpProfiler {
        #[inline(always)]
        fn items_in(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn items_out(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn net_bytes_in(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn net_bytes_out(&mut self, _from: Coord, _to: Coord, _amount: usize) {}
        #[inline(always)]
        fn iteration_boundary(&mut self, _leader_block_id: BlockId) {}
    }

    /// Get a fake profiler that does nothing.
    pub fn get_profiler() -> &'static mut NoOpProfiler {
        PROFILER.with(|t| unsafe { &mut *t.get() })
    }

    /// Do nothing, since there is nothing to wait for.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        Default::default()
    }
}

/// The implementation of the profiler when the `profiler` feature is enabled.
#[cfg(all(feature = "profiler", not(feature = "metrics")))]
mod with_profiler {
    use once_cell::sync::Lazy;
    use std::cell::UnsafeCell;
    use std::time::Instant;

    use super::bucket_profiler::BucketProfiler;
    use flume::{Receiver, Sender};

    pub use super::bucket_profiler::ProfilerResult;

    /// The sender and receiver pair of the current profilers.
    ///
    /// These are options since they can be consumed.
    static CHANNEL: Lazy<(ProfilerSender, ProfilerReceiver)> = Lazy::new(|| flume::unbounded());

    /// The sender and receiver pair of the current profilers.
    ///
    /// These are options since they can be consumed.
    static START_TIME: Lazy<Instant> = Lazy::new(|| Instant::now());

    thread_local! {
        /// The actual profiler for the current thread, if the `profiler` feature is enabled.
        static PROFILER: UnsafeCell<BucketProfiler> = UnsafeCell::new(BucketProfiler::new(*START_TIME));
    }

    /// The type of the channel sender with the `ProfilerResult`s.
    type ProfilerSender = Sender<ProfilerResult>;
    /// The type of the channel receiver with the `ProfilerResult`s.
    type ProfilerReceiver = Receiver<ProfilerResult>;

    /// Get the sender for sending the profiler results.
    pub(crate) fn get_sender() -> ProfilerSender {
        CHANNEL.0.clone()
    }

    /// Get the current profiler.
    pub fn get_profiler() -> &'static mut BucketProfiler {
        PROFILER.with(|t| unsafe { &mut *t.get() })
    }

    /// Wait for all the threads that used the profiler to exit, collect all their data and reset
    /// the profiler.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        CHANNEL.1.drain().collect()
    }
}

/// The implementation of the profiler when the `metrics` feature is enabled.
#[cfg(feature = "metrics")]
mod with_metrics {
    use once_cell::sync::Lazy;
    use std::cell::UnsafeCell;
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use std::time::Instant;

    use crate::block::CoordHasherBuilder;
    use crate::network::Coord;
    use crate::profiler::*;
    use crate::scheduler::BlockId;

    pub type TimePoint = u32;

    static RESOLUTION_MS: OnceLock<TimePoint> = OnceLock::new();

    fn bucket_resolution_ms() -> TimePoint {
        *RESOLUTION_MS.get_or_init(|| {
            std::env::var("RENOIR_METRICS_RESOLUTION_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(50)
        })
    }

    #[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
    pub struct ProfilerResult;

    /// The metrics profiler that buffers values locally and flushes to the `metrics` crate.
    #[derive(Debug, Clone)]
    pub struct MetricsProfiler {
        start: Instant,
        last_flush_ms: TimePoint,
        items_in: HashMap<(Coord, Coord), usize, CoordHasherBuilder>,
        items_out: HashMap<(Coord, Coord), usize, CoordHasherBuilder>,
        bytes_in: HashMap<(Coord, Coord), usize, CoordHasherBuilder>,
        net_messages_in: HashMap<(Coord, Coord), usize, CoordHasherBuilder>,
        bytes_out: HashMap<(Coord, Coord), usize, CoordHasherBuilder>,
        net_messages_out: HashMap<(Coord, Coord), usize, CoordHasherBuilder>,
        iteration_boundaries: HashMap<BlockId, usize>,
    }

    impl MetricsProfiler {
        pub fn new(start: Instant) -> Self {
            Self {
                start,
                last_flush_ms: 0,
                items_in: HashMap::default(),
                items_out: HashMap::default(),
                bytes_in: HashMap::default(),
                net_messages_in: HashMap::default(),
                bytes_out: HashMap::default(),
                net_messages_out: HashMap::default(),
                iteration_boundaries: HashMap::default(),
            }
        }

        #[inline]
        fn now(&self) -> TimePoint {
            self.start.elapsed().as_millis() as TimePoint
        }

        #[inline]
        fn check_flush(&mut self) {
            let now = self.now();
            if now >= self.last_flush_ms + bucket_resolution_ms() {
                self.flush_at(now);
            }
        }

        fn flush_at(&mut self, now: TimePoint) {
            // Flush items_in
            for (&(from, to), &amount) in &self.items_in {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_items_in",
                        "from_block" => from.block_id.to_string(),
                        "from_host" => from.host_id.to_string(),
                        "from_replica" => from.replica_id.to_string(),
                        "to_block" => to.block_id.to_string(),
                        "to_host" => to.host_id.to_string(),
                        "to_replica" => to.replica_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.items_in.clear();

            // Flush items_out
            for (&(from, to), &amount) in &self.items_out {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_items_out",
                        "from_block" => from.block_id.to_string(),
                        "from_host" => from.host_id.to_string(),
                        "from_replica" => from.replica_id.to_string(),
                        "to_block" => to.block_id.to_string(),
                        "to_host" => to.host_id.to_string(),
                        "to_replica" => to.replica_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.items_out.clear();

            // Flush bytes_in
            for (&(from, to), &amount) in &self.bytes_in {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_bytes_in",
                        "from_block" => from.block_id.to_string(),
                        "from_host" => from.host_id.to_string(),
                        "from_replica" => from.replica_id.to_string(),
                        "to_block" => to.block_id.to_string(),
                        "to_host" => to.host_id.to_string(),
                        "to_replica" => to.replica_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.bytes_in.clear();

            // Flush net_messages_in
            for (&(from, to), &amount) in &self.net_messages_in {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_net_messages_in",
                        "from_block" => from.block_id.to_string(),
                        "from_host" => from.host_id.to_string(),
                        "from_replica" => from.replica_id.to_string(),
                        "to_block" => to.block_id.to_string(),
                        "to_host" => to.host_id.to_string(),
                        "to_replica" => to.replica_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.net_messages_in.clear();

            // Flush bytes_out
            for (&(from, to), &amount) in &self.bytes_out {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_bytes_out",
                        "from_block" => from.block_id.to_string(),
                        "from_host" => from.host_id.to_string(),
                        "from_replica" => from.replica_id.to_string(),
                        "to_block" => to.block_id.to_string(),
                        "to_host" => to.host_id.to_string(),
                        "to_replica" => to.replica_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.bytes_out.clear();

            // Flush net_messages_out
            for (&(from, to), &amount) in &self.net_messages_out {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_net_messages_out",
                        "from_block" => from.block_id.to_string(),
                        "from_host" => from.host_id.to_string(),
                        "from_replica" => from.replica_id.to_string(),
                        "to_block" => to.block_id.to_string(),
                        "to_host" => to.host_id.to_string(),
                        "to_replica" => to.replica_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.net_messages_out.clear();

            // Flush iteration_boundaries
            for (&leader_block_id, &amount) in &self.iteration_boundaries {
                if amount > 0 {
                    metrics::counter!(
                        "renoir_iteration_boundary",
                        "leader_block" => leader_block_id.to_string(),
                    )
                    .increment(amount as u64);
                }
            }
            self.iteration_boundaries.clear();

            self.last_flush_ms = now;
        }
    }

    impl Drop for MetricsProfiler {
        fn drop(&mut self) {
            let now = self.now();
            self.flush_at(now);
        }
    }

    impl Profiler for MetricsProfiler {
        #[inline]
        fn items_in(&mut self, from: Coord, to: Coord, amount: usize) {
            *self.items_in.entry((from, to)).or_default() += amount;
            self.check_flush();
        }

        #[inline]
        fn items_out(&mut self, from: Coord, to: Coord, amount: usize) {
            *self.items_out.entry((from, to)).or_default() += amount;
            self.check_flush();
        }

        #[inline]
        fn net_bytes_in(&mut self, from: Coord, to: Coord, amount: usize) {
            *self.bytes_in.entry((from, to)).or_default() += amount;
            *self.net_messages_in.entry((from, to)).or_default() += 1;
            self.check_flush();
        }

        #[inline]
        fn net_bytes_out(&mut self, from: Coord, to: Coord, amount: usize) {
            *self.bytes_out.entry((from, to)).or_default() += amount;
            *self.net_messages_out.entry((from, to)).or_default() += 1;
            self.check_flush();
        }

        #[inline]
        fn iteration_boundary(&mut self, leader_block_id: BlockId) {
            *self.iteration_boundaries.entry(leader_block_id).or_default() += 1;
            self.check_flush();
        }
    }

    static START_TIME: Lazy<Instant> = Lazy::new(|| Instant::now());

    thread_local! {
        static PROFILER: UnsafeCell<MetricsProfiler> = UnsafeCell::new(MetricsProfiler::new(*START_TIME));
    }

    /// Get the current profiler.
    pub fn get_profiler() -> &'static mut MetricsProfiler {
        PROFILER.with(|t| unsafe { &mut *t.get() })
    }

    /// Return an empty list of results since metrics are handled globally.
    pub fn wait_profiler() -> Vec<ProfilerResult> {
        Vec::new()
    }
}
