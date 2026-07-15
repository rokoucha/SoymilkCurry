use std::{
    collections::HashSet,
    io,
    os::unix::process::CommandExt,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::{OwnedSemaphorePermit, Semaphore, broadcast, watch},
};
use tokio_stream::wrappers::{BroadcastStream, errors::BroadcastStreamRecvError};
use tracing::{error, info, warn};

use crate::config::{ChannelConfig, Config, TunerConfig};

const STREAM_BUFFER_CHUNKS: usize = 64;
const READ_BUFFER_BYTES: usize = 64 * 1024;
static NEXT_USER_ID: AtomicU64 = AtomicU64::new(1);

struct Tuner {
    config: TunerConfig,
    semaphore: Arc<Semaphore>,
    active: Mutex<Option<ActiveStream>>,
}

struct ActiveStream {
    pid: u32,
    channel_type: String,
    channel: String,
    sender: broadcast::Sender<Bytes>,
    cancel: watch::Sender<bool>,
    users: HashSet<String>,
}

pub(crate) struct TunerSnapshot {
    pub(crate) index: usize,
    pub(crate) name: String,
    pub(crate) types: Vec<String>,
    pub(crate) command: String,
    pub(crate) pid: i64,
    pub(crate) users: Vec<String>,
    pub(crate) is_free: bool,
}

pub(crate) struct OpenedStream {
    pub(crate) stream: ClientStream,
    pub(crate) user_id: String,
}

pub(crate) enum OpenError {
    NotFound,
    Unavailable,
    Spawn,
}

pub(crate) struct AppState {
    config: Config,
    tuners: Vec<Arc<Tuner>>,
}

impl AppState {
    pub(crate) fn new(config: Config) -> Self {
        let tuners = config
            .tuners
            .iter()
            .cloned()
            .map(|config| {
                Arc::new(Tuner {
                    config,
                    semaphore: Arc::new(Semaphore::new(1)),
                    active: Mutex::new(None),
                })
            })
            .collect();
        Self { config, tuners }
    }

    pub(crate) fn channels(&self) -> &[ChannelConfig] {
        &self.config.channels
    }

    pub(crate) fn channel(&self, channel_type: &str, channel: &str) -> Option<&ChannelConfig> {
        self.config
            .channels
            .iter()
            .find(|item| item.channel_type == channel_type && item.channel == channel)
    }

    pub(crate) fn stream_count(&self) -> usize {
        self.tuners
            .iter()
            .filter(|tuner| tuner.semaphore.available_permits() == 0)
            .count()
    }

    pub(crate) fn tuner_snapshots(&self) -> Vec<TunerSnapshot> {
        self.tuners
            .iter()
            .enumerate()
            .map(|(index, tuner)| {
                let active = tuner.active.lock().expect("active stream mutex poisoned");
                TunerSnapshot {
                    index,
                    name: tuner.config.name.clone(),
                    types: tuner.config.types.clone(),
                    command: tuner.config.command.clone(),
                    pid: active.as_ref().map_or(-1, |stream| i64::from(stream.pid)),
                    users: active
                        .as_ref()
                        .map(|stream| stream.users.iter().cloned().collect())
                        .unwrap_or_default(),
                    is_free: tuner.semaphore.available_permits() > 0,
                }
            })
            .collect()
    }

    pub(crate) fn stream_available(&self, channel_type: &str, channel: &str) -> bool {
        self.tuners.iter().any(|tuner| {
            if !tuner.config.types.iter().any(|value| value == channel_type) {
                return false;
            }
            let active = tuner.active.lock().expect("active stream mutex poisoned");
            active.as_ref().is_some_and(|stream| {
                stream.channel_type == channel_type && stream.channel == channel
            }) || active.is_none()
        })
    }

    pub(crate) fn open_stream(
        self: &Arc<Self>,
        channel_type: &str,
        channel: &str,
    ) -> Result<OpenedStream, OpenError> {
        let channel_config = self
            .channel(channel_type, channel)
            .cloned()
            .ok_or(OpenError::NotFound)?;
        let user_id = format!(
            "soymilk-curry-{}-{}",
            unix_time_ms(),
            NEXT_USER_ID.fetch_add(1, Ordering::Relaxed)
        );

        for tuner in &self.tuners {
            if !tuner.config.types.iter().any(|value| value == channel_type) {
                continue;
            }
            let mut active = tuner.active.lock().expect("active stream mutex poisoned");
            if let Some(stream) = active.as_mut() {
                if stream.channel_type == channel_type && stream.channel == channel {
                    let receiver = stream.sender.subscribe();
                    stream.users.insert(user_id.clone());
                    info!(
                        tuner = %tuner.config.name,
                        pid = stream.pid,
                        %channel_type,
                        %channel,
                        users = stream.users.len(),
                        "stream shared"
                    );
                    return Ok(OpenedStream {
                        stream: ClientStream::new(receiver, Arc::clone(tuner), user_id.clone()),
                        user_id,
                    });
                }
                continue;
            }
            let Ok(permit) = tuner.semaphore.clone().try_acquire_owned() else {
                continue;
            };
            let (child, stdout) = spawn_tuner_command(tuner, &channel_config)?;
            let pid = child.id().ok_or(OpenError::Spawn)?;
            let (sender, receiver) = broadcast::channel(STREAM_BUFFER_CHUNKS);
            let (cancel, cancel_receiver) = watch::channel(false);
            let users = HashSet::from([user_id.clone()]);
            *active = Some(ActiveStream {
                pid,
                channel_type: channel_type.to_owned(),
                channel: channel.to_owned(),
                sender: sender.clone(),
                cancel,
                users,
            });
            info!(tuner = %tuner.config.name, %pid, %channel_type, %channel, "stream started");
            tokio::spawn(pump_tuner(
                Arc::clone(tuner),
                child,
                stdout,
                sender,
                cancel_receiver,
                permit,
                pid,
            ));
            drop(active);

            return Ok(OpenedStream {
                stream: ClientStream::new(receiver, Arc::clone(tuner), user_id.clone()),
                user_id,
            });
        }
        Err(OpenError::Unavailable)
    }

    #[cfg(test)]
    pub(crate) fn tuner_is_free(&self, index: usize) -> bool {
        self.tuners[index].semaphore.available_permits() > 0
    }
}

fn spawn_tuner_command(
    tuner: &Tuner,
    channel: &ChannelConfig,
) -> Result<(Child, tokio::process::ChildStdout), OpenError> {
    let command_line = render_command(&tuner.config.command, channel);
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg(command_line)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);

    let mut child = command.spawn().map_err(|error| {
        error!(tuner = %tuner.config.name, %error, "failed to spawn tuner command");
        OpenError::Spawn
    })?;
    let stdout = child.stdout.take().ok_or(OpenError::Spawn)?;
    Ok((child, stdout))
}

async fn pump_tuner(
    tuner: Arc<Tuner>,
    child: Child,
    mut stdout: tokio::process::ChildStdout,
    sender: broadcast::Sender<Bytes>,
    mut cancel: watch::Receiver<bool>,
    permit: OwnedSemaphorePermit,
    pid: u32,
) {
    let process = TunerProcess { _child: child, pid };
    let mut buffer = vec![0; READ_BUFFER_BYTES];
    loop {
        tokio::select! {
            result = stdout.read(&mut buffer) => match result {
                Ok(0) => break,
                Ok(length) => {
                    if sender.send(Bytes::copy_from_slice(&buffer[..length])).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    warn!(tuner = %tuner.config.name, %pid, %error, "failed to read tuner output");
                    break;
                }
            },
            result = cancel.changed() => {
                if result.is_err() || *cancel.borrow() {
                    break;
                }
            }
        }
    }

    drop(process);
    let mut active = tuner.active.lock().expect("active stream mutex poisoned");
    if active.as_ref().is_some_and(|stream| stream.pid == pid) {
        *active = None;
    }
    drop(active);
    drop(permit);
    info!(tuner = %tuner.config.name, %pid, "stream stopped");
}

fn render_command(template: &str, channel: &ChannelConfig) -> String {
    let mut command = template
        .replace("<type>", &shell_quote(&channel.channel_type))
        .replace("<channel>", &shell_quote(&channel.channel));
    if let Some(service_id) = channel.service_id {
        command = command.replace("<serviceId>", &service_id.to_string());
    }
    for (key, value) in &channel.command_vars {
        command = command.replace(&format!("<{key}>"), &shell_quote(value));
    }
    command
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct TunerProcess {
    _child: Child,
    pid: u32,
}

impl Drop for TunerProcess {
    fn drop(&mut self) {
        if let Err(error) = killpg(Pid::from_raw(self.pid as i32), Signal::SIGKILL)
            && error != nix::errno::Errno::ESRCH
        {
            warn!(pid = self.pid, %error, "failed to kill tuner process group");
        }
    }
}

pub(crate) struct ClientStream {
    receiver: BroadcastStream<Bytes>,
    tuner: Arc<Tuner>,
    user_id: String,
}

impl ClientStream {
    fn new(receiver: broadcast::Receiver<Bytes>, tuner: Arc<Tuner>, user_id: String) -> Self {
        Self {
            receiver: BroadcastStream::new(receiver),
            tuner,
            user_id,
        }
    }
}

impl futures_core::Stream for ClientStream {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                Poll::Ready(Some(Err(io::Error::other(format!(
                    "stream client lagged by {skipped} chunks"
                )))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ClientStream {
    fn drop(&mut self) {
        let mut active = self
            .tuner
            .active
            .lock()
            .expect("active stream mutex poisoned");
        if let Some(stream) = active.as_mut() {
            stream.users.remove(&self.user_id);
            if stream.users.is_empty() {
                let _ = stream.cancel.send(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn command_variables_are_shell_quoted() {
        let channel = ChannelConfig {
            name: "test".into(),
            channel_type: "GR".into(),
            channel: "27'; touch /tmp/nope; echo '".into(),
            service_id: Some(1024),
            command_vars: BTreeMap::from([("device".into(), "/dev/dvb 0".into())]),
        };
        assert_eq!(
            render_command("record <type> <channel> <serviceId> <device>", &channel),
            "record 'GR' '27'\"'\"'; touch /tmp/nope; echo '\"'\"'' 1024 '/dev/dvb 0'"
        );
    }
}
