//! Work an app does without anyone asking: refreshing a cache, pulling from
//! an API, tidying a table.
//!
//! A job is a cron expression and a path. When it fires, the host calls the
//! app's own handler exactly as a request would — same sandbox, same limits,
//! same database, no identity. So a job is just a route the app already has,
//! and nothing new has to be reasoned about to know what it can do.

use crate::{config::Config, content::slug::valid_slug, AppState};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

/// How often the scheduler looks for work unless the site says otherwise
/// (`TOOLSITE_SCHEDULER_TICK_SECS`). Jobs fire at most once per tick, so
/// this is also the coarsest resolution a schedule really has.
pub const DEFAULT_TICK: Duration = Duration::from_secs(30);

/// The shortest tick a site may ask for. A schedule's resolution is a
/// second, so anything finer would only wake the process to find nothing.
pub const MIN_TICK: Duration = Duration::from_secs(1);

/// A job that runs long enough to be stuck is not retried on the next tick;
/// it is left alone and reported.
const JOB_GUARDS: crate::runtime::wasm::Guards = crate::runtime::wasm::Guards {
    fuel: 2_000_000_000,
    memory_bytes: 128 * 1024 * 1024,
    wall_clock: Duration::from_secs(60),
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Standard cron with seconds leading, as the `cron` crate reads it.
    pub schedule: String,
    /// The path handed to the handler, e.g. `/api/refresh`.
    pub path: String,
    /// Unix seconds. Absent until it has run once.
    #[serde(default)]
    pub last_run: Option<u64>,
    /// What happened last time, for whoever is wondering why nothing changed.
    #[serde(default)]
    pub last_status: Option<String>,
}

fn path_for(config: &Config, app: &str) -> Option<PathBuf> {
    valid_slug(app).then(|| config.data_dir.join(format!("{app}.jobs")))
}

pub fn read_jobs(config: &Config, app: &str) -> BTreeMap<String, Job> {
    let Some(path) = path_for(config, app) else {
        return BTreeMap::new();
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

fn write_jobs(config: &Config, app: &str, jobs: &BTreeMap<String, Job>) -> Result<(), String> {
    let path = path_for(config, app).ok_or_else(|| format!("invalid app name '{app}'"))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let json = serde_json::to_string_pretty(jobs).map_err(|e| e.to_string())?;
    std::fs::write(path, json).map_err(|e| e.to_string())
}

/// Adds or replaces a job. The schedule is parsed here so a bad expression is
/// refused while someone is watching, rather than silently never firing.
pub fn set_job(
    config: &Config,
    app: &str,
    name: &str,
    schedule: &str,
    path: &str,
) -> Result<String, String> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        return Err("a job's name must be letters, numbers, '-' or '_'".into());
    }
    if !path.starts_with('/') {
        return Err("path must start with '/', e.g. /api/refresh".into());
    }
    let parsed = cron::Schedule::from_str(schedule).map_err(|e| {
        format!("{e}. Six fields, seconds first — '0 */5 * * * *' is every five minutes")
    })?;
    let next = parsed
        .upcoming(Utc)
        .next()
        .ok_or("that schedule never fires")?;

    let mut jobs = read_jobs(config, app);
    jobs.insert(
        name.to_string(),
        Job {
            schedule: schedule.to_string(),
            path: path.to_string(),
            last_run: None,
            last_status: None,
        },
    );
    write_jobs(config, app, &jobs)?;
    Ok(format!("next run {}", next.to_rfc3339()))
}

pub fn remove_job(config: &Config, app: &str, name: &str) -> Result<(), String> {
    let mut jobs = read_jobs(config, app);
    if jobs.remove(name).is_none() {
        return Err(format!("{app} has no job called {name}"));
    }
    write_jobs(config, app, &jobs)
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Whether a job is due at `at`, given when it last ran.
///
/// The question asked is "did a scheduled time pass since we last ran", not
/// "is it that second now" — a tick every thirty seconds would otherwise miss
/// almost everything. `since_tick` is where the tick's own window opens,
/// which is what a job that has never run is measured from.
fn is_due(job: &Job, at: u64, since_tick: u64) -> bool {
    let Ok(schedule) = cron::Schedule::from_str(&job.schedule) else {
        return false;
    };
    // From the last run, so a job that missed its turn while the server was
    // down still fires once. With no last run the window is the tick's, so
    // creating a yearly job does not set it off immediately.
    let since = job.last_run.unwrap_or(since_tick);
    let Some(after) = chrono::DateTime::from_timestamp(since as i64, 0) else {
        return false;
    };
    schedule
        .after(&after)
        .next()
        .is_some_and(|next| next.timestamp() as u64 <= at)
}

/// Runs one job now, whatever its schedule says, and records the outcome.
/// This is also what an on-demand run uses, so the two cannot behave
/// differently.
pub async fn run_job(state: &AppState, app: &str, name: &str) -> Result<String, String> {
    let jobs = read_jobs(&state.config, app);
    let job = jobs.get(name).cloned().ok_or_else(|| format!("{app} has no job called {name}"))?;

    let wasm = tokio::fs::read(state.config.data_dir.join(app).join("handler.wasm"))
        .await
        .map_err(|_| format!("{app} has no handler to run"))?;

    let request = crate::runtime::wasm::Request {
        method: "GET".to_string(),
        path: job.path.clone(),
        query: String::new(),
        // Says plainly that nobody is waiting on the other end, so a handler
        // can behave differently if it wants to.
        headers: vec![("x-toolsite-scheduled".to_string(), name.to_string())],
        body: Vec::new(),
    };

    let runtime = state.runtime.clone();
    let config = state.config.clone();
    let owned_app = app.to_string();
    let outcome = tokio::task::spawn_blocking(move || {
        // No identity: a scheduled run is the app acting on its own behalf.
        runtime.handle(config, &owned_app, &wasm, None, request, JOB_GUARDS)
    })
    .await;

    let status = match outcome {
        Ok(Ok(response)) => format!("{}", response.status),
        Ok(Err(error)) => format!("failed: {error}"),
        Err(error) => format!("failed: {error}"),
    };

    record_run(&state.config, app, name, &status);
    Ok(status)
}

/// Records an outcome against a job. Used by the scheduler, and by tests
/// that need a job to look as though it has run.
pub fn record_run(config: &Config, app: &str, name: &str, status: &str) {
    let mut jobs = read_jobs(config, app);
    if let Some(job) = jobs.get_mut(name) {
        job.last_run = Some(now());
        job.last_status = Some(status.to_string());
        let _ = write_jobs(config, app, &jobs);
    }
}

/// Every app that has jobs, found the same way the index finds pages.
async fn apps_with_jobs(config: &Config) -> Vec<String> {
    let Ok(mut entries) = tokio::fs::read_dir(&config.data_dir).await else {
        return Vec::new();
    };
    let mut apps = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(app) = name.strip_suffix(".jobs") {
            apps.push(app.to_string());
        }
    }
    apps
}

/// Where a tick at `at` opens its window for jobs that have never run: at
/// the tick before it, so one tick's window ends where the next begins and
/// every second is looked at exactly once, however late a tick wakes. The
/// first tick, or one whose predecessor is somehow later, looks back one
/// tick.
fn window_start(previous: Option<u64>, at: u64, tick: Duration) -> u64 {
    match previous {
        Some(previous) if previous <= at => previous,
        _ => at.saturating_sub(tick.as_secs()),
    }
}

/// What runs an app's jobs on their schedules. A process starts one, from
/// `main`, for the data directory it serves: two would each fire every job.
#[derive(Clone)]
pub struct Scheduler {
    state: AppState,
    /// `app:job` for each job in progress, so a slow one is skipped rather
    /// than stacked.
    running: Arc<Mutex<HashSet<String>>>,
    /// How long it sleeps between looks.
    period: Duration,
    /// When the previous tick looked, in Unix seconds.
    last_tick: Arc<Mutex<Option<u64>>>,
}

impl Scheduler {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            running: Arc::default(),
            period: DEFAULT_TICK,
            last_tick: Arc::default(),
        }
    }

    /// Looks for work every `tick` instead of every `DEFAULT_TICK`, but
    /// never more often than `MIN_TICK`.
    pub fn every(self, tick: Duration) -> Self {
        Self {
            period: tick.max(MIN_TICK),
            ..self
        }
    }

    /// Wakes every tick and starts whatever is due.
    pub fn spawn(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(self.period).await;
                self.tick(now()).await;
            }
        });
    }

    /// Starts every job due at `at`, in Unix seconds, that is not still
    /// running from an earlier tick. Returns the runs it started, which
    /// finish on their own; awaiting them is for whoever wants to know when.
    pub async fn tick(&self, at: u64) -> Vec<tokio::task::JoinHandle<()>> {
        let previous = self.last_tick.lock().unwrap().replace(at);
        let since_tick = window_start(previous, at, self.period);
        let mut started = Vec::new();
        for app in apps_with_jobs(&self.state.config).await {
            for (name, job) in read_jobs(&self.state.config, &app) {
                if !is_due(&job, at, since_tick) {
                    continue;
                }
                let key = format!("{app}:{name}");
                if !self.running.lock().unwrap().insert(key.clone()) {
                    tracing::warn!(app, job = name, "still running; skipping this turn");
                    continue;
                }
                let (state, running, app) = (self.state.clone(), self.running.clone(), app.clone());
                started.push(tokio::spawn(async move {
                    match run_job(&state, &app, &name).await {
                        Ok(status) => tracing::info!(app, job = name, status, "job ran"),
                        Err(error) => tracing::warn!(app, job = name, error, "job failed"),
                    }
                    running.lock().unwrap().remove(&key);
                }));
            }
        }
        started
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> (tempfile::TempDir, Config) {
        let dir = tempfile::tempdir().unwrap();
        let config = Config::local(dir.path().to_path_buf(), "test-token");
        (dir, config)
    }

    #[test]
    fn a_schedule_is_checked_when_it_is_set_not_when_it_should_fire() {
        let (_dir, config) = config();
        assert!(set_job(&config, "app", "refresh", "0 */5 * * * *", "/api/refresh").is_ok());

        let error = set_job(&config, "app", "refresh", "not a schedule", "/api/x").unwrap_err();
        assert!(error.contains("Six fields"), "the error should show the shape: {error}");
        // A path a handler could never receive is refused too.
        assert!(set_job(&config, "app", "refresh", "0 * * * * *", "api/x").is_err());
    }

    #[test]
    fn a_job_is_due_when_a_scheduled_time_has_passed_since_it_last_ran() {
        // Fixed instants rather than the wall clock: on a minute schedule,
        // "a second ago" is or is not due depending on where in the minute
        // the test happens to run.
        const BOUNDARY: u64 = 1_000_000_020; // divisible by 60
        let at = BOUNDARY + 10;
        let every_minute = Job {
            schedule: "0 * * * * *".to_string(),
            path: "/api/x".to_string(),
            last_run: None,
            last_status: None,
        };

        // Last ran before the boundary, so that minute came round: due.
        let missed = Job {
            last_run: Some(BOUNDARY - 20),
            ..every_minute.clone()
        };
        let since_tick = at - DEFAULT_TICK.as_secs();
        assert!(is_due(&missed, at, since_tick));

        // Ran after it, and the next one has not arrived: not due.
        let recent = Job {
            last_run: Some(BOUNDARY + 5),
            ..every_minute.clone()
        };
        assert!(!is_due(&recent, at, since_tick));

        // Down for an hour: it fires once, not once per missed minute.
        let long_gone = Job {
            last_run: Some(BOUNDARY - 3600),
            ..every_minute
        };
        assert!(is_due(&long_gone, at, since_tick));
    }

    #[test]
    fn a_yearly_job_is_not_due_just_because_it_never_ran() {
        let new_year = Job {
            schedule: "0 0 0 1 1 *".to_string(),
            path: "/api/x".to_string(),
            last_run: None,
            last_status: None,
        };
        // Without a last run the window is one tick, not all of history, so a
        // rare job does not fire the moment it is created.
        let at = now();
        assert!(!is_due(&new_year, at, window_start(None, at, DEFAULT_TICK)));
    }

    /// Ticks the way the loop does, `period` apart plus however late each
    /// one wakes, and records a run at the tick that started it. Returns the
    /// ticks at which the job fired.
    fn fires(job: &Job, period: Duration, ticks: impl Iterator<Item = u64>) -> Vec<u64> {
        let mut job = job.clone();
        let mut previous = None;
        let mut fired = Vec::new();
        for at in ticks {
            let since_tick = window_start(previous, at, period);
            previous = Some(at);
            if is_due(&job, at, since_tick) {
                fired.push(at);
                job.last_run = Some(at);
            }
        }
        fired
    }

    #[test]
    fn a_job_that_never_ran_fires_exactly_once_for_its_moment_at_any_tick() {
        // A yearly job, created a few ticks before its moment: whatever the
        // tick, it fires once, at the first tick at or after that moment.
        const MOMENT: u64 = 1_000_000_020; // 2001-09-09 01:47:00 UTC
        let once = Job {
            schedule: "0 47 1 9 9 *".to_string(),
            path: "/api/x".to_string(),
            last_run: None,
            last_status: None,
        };
        // Each tick wakes up to two seconds late, which is what a loaded
        // machine does to `sleep(tick)` followed by the work of a tick, and
        // the ticks start at every offset, so the moment lands everywhere
        // between two of them. Measured back one tick from each, some of
        // these moments fell between the windows and never fired.
        for period in [1, 2, 5, 7, 30, 45] {
            for offset in 0..period + 3 {
                let mut at = MOMENT - 3 * period - offset;
                let ticks = std::iter::from_fn(|| {
                    let this = at;
                    at += period + (this % 3);
                    Some(this)
                })
                .take_while(|at| *at < MOMENT + 60);
                let fired = fires(&once, Duration::from_secs(period), ticks);
                assert_eq!(fired.len(), 1, "a {period}s tick from offset {offset} fired at {fired:?}");
                assert!(
                    fired[0] >= MOMENT && fired[0] < MOMENT + period + 3,
                    "a {period}s tick from offset {offset} fired late, at {fired:?}"
                );
            }
        }
    }

    #[test]
    fn a_short_tick_still_fires_a_job_once_per_scheduled_time() {
        // Every ten seconds, ticked every second for two minutes: twelve runs,
        // one per time, and never two for the same one.
        let every_ten = Job {
            schedule: "*/10 * * * * *".to_string(),
            path: "/api/x".to_string(),
            last_run: None,
            last_status: None,
        };
        const START: u64 = 1_000_000_020;
        let fired = fires(&every_ten, MIN_TICK, START + 1..=START + 120);
        assert_eq!(fired, (1..=12).map(|n| START + n * 10).collect::<Vec<_>>());
    }

    #[test]
    fn a_tick_is_never_shorter_than_a_second() {
        let (_dir, config) = config();
        let state = AppState {
            config: Arc::new(config),
            runtime: crate::runtime::wasm::Runtime::new().unwrap(),
        };
        assert_eq!(Scheduler::new(state.clone()).period, DEFAULT_TICK);
        assert_eq!(Scheduler::new(state.clone()).every(Duration::ZERO).period, MIN_TICK);
        assert_eq!(Scheduler::new(state).every(Duration::from_secs(5)).period, Duration::from_secs(5));
    }

    #[test]
    fn jobs_belong_to_one_app() {
        let (_dir, config) = config();
        set_job(&config, "mine", "refresh", "0 * * * * *", "/api/x").unwrap();
        assert!(read_jobs(&config, "theirs").is_empty());
        assert!(remove_job(&config, "theirs", "refresh").is_err());
    }
}
