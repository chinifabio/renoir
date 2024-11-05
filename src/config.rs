//! Configuration types used to initialize the [`StreamContext`](crate::StreamContext).
//!
//! See the documentation of [`RuntimeConfig`] for more details.

use std::env;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

#[cfg(feature = "clap")]
use clap::Parser;
use serde::{Deserialize, Serialize};

use crate::operator::sink::connectors::ConnectorSinkTechnology;
use crate::operator::ExchangeData;
use crate::prelude::connectors::ConnectorSourceTechnology;
use crate::runner::spawn_remote_workers;
use crate::scheduler::{HostId, HostTag};
use crate::CoordUInt;

/// Environment variable set by the runner with the host id of the process. If it's missing the
/// process will have to spawn the processes by itself.
pub const HOST_ID_ENV_VAR: &str = "RENOIR_HOST_ID";
/// Environment variable set by the runner with the content of the config file so that it's not
/// required to have it on all the hosts.
pub const CONFIG_ENV_VAR: &str = "RENOIR_CONFIG";
/// Environment variable set by the runner with the tag of the job. This is used to identify the
/// processes that are part of the same group.
pub const HOST_TAG_ENV_VAR: &str = "RENOIR_GROUP_TAG";

/// The runtime configuration of the environment,
///
/// This configuration selects which runtime to use for this execution. The runtime is either local
/// (i.e. parallelism is achieved only using threads), or remote (i.e. using both threads locally
/// and remote workers).
///
/// In a remote execution the current binary is copied using scp to a remote host and then executed
/// using ssh. The configuration of the remote environment should be specified via a TOML
/// configuration file.
///
/// ## Local environment
///
/// ```
/// # use renoir::{StreamContext, RuntimeConfig};
/// let config = RuntimeConfig::local(2).unwrap();
/// let env = StreamContext::new(config);
/// ```
///
/// ## Remote environment
///
/// ```
/// # use renoir::{StreamContext, RuntimeConfig};
/// # use std::fs::File;
/// # use std::io::Write;
/// let config = r#"
/// [[host]]
/// address = "host1"
/// base_port = 9500
/// num_cores = 16
///
/// [[host]]
/// address = "host2"
/// base_port = 9500
/// num_cores = 24
/// "#;
/// let mut file = File::create("config.toml").unwrap();
/// file.write_all(config.as_bytes());
///
/// let config = RuntimeConfig::remote("config.toml").expect("cannot read config file");
/// let env = StreamContext::new(config);
/// ```
///
/// ## From command line arguments
/// This reads from `std::env::args()` and reads the most common options (`--local`, `--remote`,
/// `--verbose`). All the unparsed options will be returned into `args`. You can use `--help` to see
/// their docs.
///
/// ```no_run
/// # use renoir::{RuntimeConfig, StreamContext};
/// let (config, args) = RuntimeConfig::from_args();
/// let env = StreamContext::new(config);
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RuntimeConfig {
    /// Use only local threads.
    Local(LocalConfig),
    /// Use both local threads and remote workers.
    Remote(RemoteConfig),
    /// Use a distributed configuration which can be divided into tiers and groups.
    Distributed(DistributedConfig),
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(|q| q.get())
            .unwrap_or(4);
        RuntimeConfig::local(parallelism as u64).unwrap()
    }
}

// #[derive(Debug, Clone, Eq, PartialEq)]
// pub struct RuntimeConfig {
//     /// Which runtime to use for the environment.
//     pub runtime: RuntimeConfig,
//     /// In a remote execution this field represents the identifier of the host, i.e. the index
//     /// inside the host list in the config.
//     pub host_id: Option<HostId>,
// }

/// This environment uses only local threads.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LocalConfig {
    /// The number of CPU cores of this host.
    ///
    /// A thread will be spawned for each core, for each block in the job graph.
    pub parallelism: CoordUInt,
}

/// This environment uses local threads and remote hosts.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct RemoteConfig {
    /// The identifier for this host.
    #[serde(skip)]
    host_id: Option<HostId>, // TODO: remove option
    /// The set of remote hosts to use.
    #[serde(rename = "host")]
    pub hosts: Vec<HostConfig>,
    /// If specified some debug information will be stored inside this directory.
    pub tracing_dir: Option<PathBuf>,
    /// Remove remote binaries after execution
    #[serde(default)]
    pub cleanup_executable: bool,
    /// The set of connections between groups of the job.
    #[serde(rename = "connector")]
    pub connectors: Vec<GroupConnector>,
}

/// This environment uses a distributed configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct DistributedConfig {
    /// The identifier for this host.
    #[serde(skip)]
    pub(crate) host_id: Option<HostId>, // TODO: remove option
    /// The tier for this host.
    #[serde(skip)]
    pub(crate) host_tier: Option<HostTag>,
    /// The group for this host.
    #[serde(skip)]
    pub(crate) host_group: Option<HostTag>,
    /// The tier of the host.
    #[serde(rename = "tier")]
    pub(crate) tiers: Vec<TierConfig>,
    /// List of connections between groups and tiers.
    pub(crate) connectors: Vec<GroupConnector>,
    /// The mode of the deployment.
    #[serde(default)]
    pub(crate) deployment_mode: DeploymentMode,

    // vec di tierconfig onguna con vec di hostconfig
    // vec di connessioni? qui o in hostconfig riguardo solamente il tier precedente?
}

/// The configuration of a single remote host.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct HostConfig {
    /// The IP address or domain name to use for connecting to this remote host.
    ///
    /// This must be reachable from all the hosts in the cluster.
    pub address: String,
    /// The first port to use for inter-host communication.
    ///
    /// This port and the following ones will be bound by the host, one for each connection between
    /// blocks of the job graph..
    pub base_port: u16,
    /// The number of cores of the remote host.
    ///
    /// This is the same as `LocalRuntimeConfig::num_cores`.
    pub num_cores: CoordUInt,
    /// The configuration to use to connect via SSH to the remote host.
    #[serde(default)]
    pub ssh: SSHConfig,
    /// If specified the remote worker will be spawned under `perf`, and its output will be stored
    /// at this location.
    pub perf_path: Option<PathBuf>,
    /// The tag identifying the cluster group to which this host belongs.
    pub tags: Vec<String>
}

/// The information used to connect to a remote host via SSH.
#[derive(Clone, Serialize, Deserialize, Derivative, Eq, PartialEq)]
#[derivative(Default)]
#[allow(clippy::upper_case_acronyms)]
pub struct SSHConfig {
    /// The SSH port this host listens to.
    #[derivative(Default(value = "22"))]
    #[serde(default = "ssh_default_port")]
    pub ssh_port: u16,
    /// The username of the remote host. Defaulted to the local username.
    pub username: Option<String>,
    /// The password of the remote host. If not specified ssh-agent will be used for the connection.
    pub password: Option<String>,
    /// The path to the private key to use for authenticating to the remote host.
    pub key_file: Option<PathBuf>,
    /// The passphrase for decrypting the private SSH key.
    pub key_passphrase: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct TierConfig {
    /// The name of the tier.
    pub name: String,
    /// The group of the tier.
    #[serde(rename = "group")]
    pub groups: Vec<GroupConfig>,

    /// The groups available for the tier.
    // pub groups: Vec<HostTag>,
    /// The hosts of the tier.
    // pub hosts: Vec<HostConfig>,
    /// The connections between the hosts of this tier.
    // pub connectors: Vec<GroupConnector>,
    pub tags: Vec<String> // todo this is a placeholder
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct GroupConfig {
    pub name: String,
    #[serde(rename = "host")]
    pub hosts: Vec<HostConfig>,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum DeploymentMode {
    /// Connects to nodes and spawns workers processes using SSH and SCP.
    #[default]
    Legacy,
    /// Connects to nodes and spawns something. spoiler: it's a TODO.
    Service
}

impl std::fmt::Debug for SSHConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Ssh");
        if self.ssh_port != 22 {
            d.field("port", &self.ssh_port);
        }
        if let Some(username) = &self.username {
            d.field("username", &username);
        }
        if let Some(key_file) = &self.key_file {
            d.field("key_file", &key_file);
        }
        if self.password.is_some() {
            d.field("password", &"REDACTED");
        }
        if self.key_passphrase.is_some() {
            d.field("key_passphrase", &"REDACTED");
        }

        d.finish()
    }
}

#[cfg(feature = "clap")]
#[derive(Debug, Parser)]
#[clap(
    name = "renoir",
    about = "Network of Operators In Rust",
    trailing_var_arg = true
)]
pub struct CommandLineOptions {
    /// Path to the configuration file for remote execution.
    ///
    /// When this is specified the execution will be remote. This conflicts with `--local`.
    #[clap(short, long)]
    remote: Option<PathBuf>,

    /// Number of cores in the local execution.
    ///
    /// When this is specified the execution will be local. This conflicts with `--remote`.
    #[clap(short, long)]
    local: Option<CoordUInt>,

    /// The rest of the arguments.
    args: Vec<String>,
}

impl RuntimeConfig {
    /// Build the configuration from the specified args list.
    #[cfg(feature = "clap")]
    pub fn from_args() -> (RuntimeConfig, Vec<String>) {
        let opt: CommandLineOptions = CommandLineOptions::parse();
        opt.validate();

        let mut args = opt.args;
        args.insert(0, env::args().next().unwrap());

        if let Some(parallelism) = opt.local {
            (Self::local(parallelism).expect("Configuration error"), args)
        } else if let Some(remote) = opt.remote {
            (Self::remote(remote).expect("Configuration error"), args)
        } else {
            unreachable!("Invalid configuration")
        }
    }

    /// Local environment that avoid using the network and runs concurrently using only threads.
    pub fn local(parallelism: CoordUInt) -> Result<RuntimeConfig, ConfigError> {
        ConfigBuilder::new_local(parallelism)
    }

    /// Remote environment based on the provided configuration file.
    ///
    /// The behaviour of this changes if this process is the "runner" process (ie the one that will
    /// execute via ssh the other workers) or a worker process.
    /// If it's the runner, the configuration file is read. If it's a worker, the configuration is
    /// read directly from the environment variable and not from the file (remote hosts may not have
    /// the configuration file).
    pub fn remote<P: AsRef<Path>>(toml_path: P) -> Result<RuntimeConfig, ConfigError> {
        let mut builder = ConfigBuilder::new_remote();

        if env::var(CONFIG_ENV_VAR).is_ok() {
            builder.parse_env()?;
            builder.host_id_from_env()?;
            builder.host_tag_from_env()?;
        } else {
            builder.parse_file(toml_path)?;
        }

        builder.build()
    }

    /// Spawn the remote workers via SSH and exit if this is the process that should spawn. If this
    /// is already a spawned process nothing is done.
    pub fn spawn_remote_workers(&self) {
        match &self {
            RuntimeConfig::Local(_) => {}
            #[cfg(feature = "ssh")]
            RuntimeConfig::Remote(remote) => {
                spawn_remote_workers(remote.clone());
            }
            #[cfg(not(feature = "ssh"))]
            RuntimeConfig::Remote(_) => {
                panic!("spawn_remote_workers() requires the `ssh` feature for remote configs.");
            }
            RuntimeConfig::Distributed(distributed) => {
                match distributed.deployment_mode {
                    DeploymentMode::Legacy => {
                        spawn_remote_workers(distributed.into());
                    }
                    DeploymentMode::Service => {
                        todo!("create a new spawn function, maybe using systemd o some kind of orchestrator");
                    }
                }
            }
        }
    }

    pub fn host_id(&self) -> Option<HostId> {
        match self {
            RuntimeConfig::Local(_) => Some(0),
            RuntimeConfig::Remote(remote) => remote.host_id,
            RuntimeConfig::Distributed(distributed) => distributed.host_id,
        }
    }

    /// The tag of the host.
    pub fn host_tier(&self) -> Option<String> {
        match self {
            RuntimeConfig::Distributed(config) => config.host_tier.clone(),
            _ => None,
        }
    }

    /// The group of the host.
    pub fn host_group(&self) -> Option<String> {
        match self {
            RuntimeConfig::Distributed(config) => config.host_group.clone(),
            _ => None,
        }
    }

    /// TODO docs
    pub(crate) fn get_connector(
        &self,
        from: impl Into<String>,
        to: impl Into<String>,
    ) -> GroupConnector {
        return match self {
            RuntimeConfig::Distributed(config) => {
                let from = from.into();
                let to = to.into();
                config
                    .connectors
                    .iter()
                    .find(|c| c.from == from && c.to == to)
                    .cloned()
                    .unwrap_or_else(|| {
                        panic!(
                            "Missing connector from {} to {} in the distributed configuration",
                            from, to
                        )
                    })
            },
            _ => panic!("Distributed configuration required"),
        };
    }
}

impl Display for HostConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}:{}-]", self.address, self.base_port)
    }
}

#[cfg(feature = "clap")]
impl CommandLineOptions {
    /// Check that the configuration provided is valid.
    fn validate(&self) {
        if !(self.remote.is_some() ^ self.local.is_some()) {
            panic!("Use one of --remote or --local");
        }
    }
}
#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    host_id: Option<HostId>,
    host_tag: Option<HostTag>,
    hosts: Vec<HostConfig>,
    tracing_dir: Option<PathBuf>,
    cleanup_executable: bool,
    connectors: Vec<GroupConnector>, // TODO: implement methods to manage connectors
}

impl ConfigBuilder {
    pub fn new_local(parallelism: CoordUInt) -> Result<RuntimeConfig, ConfigError> {
        if parallelism == 0 {
            Err(ConfigError::Invalid(
                "The number of cores should be positive".into(),
            ))
        } else {
            Ok(RuntimeConfig::Local(LocalConfig { parallelism }))
        }
    }

    pub fn new_remote() -> Self {
        Self {
            host_id: None,
            host_tag: None,
            hosts: Vec::new(),
            tracing_dir: None,
            cleanup_executable: false,
            connectors: Vec::new(),
        }
    }
    /// Parse toml and integrate it in the builder.
    /// Hosts are appended to the list, the rest of the parameters set only if they were not present.
    /// host_id is ignored. Configure it directly
    pub fn parse_toml_str(&mut self, config_str: &str) -> Result<&mut Self, ConfigError> {
        let RemoteConfig {
            host_id: _, // Ignore serialized host_id
            hosts,
            tracing_dir,
            cleanup_executable,
            connectors,
        } = toml::from_str(config_str)?;

        // validate the configuration
        for host in hosts.into_iter() {
            if host.ssh.password.is_some() && host.ssh.key_file.is_some() {
                return Err(ConfigError::Invalid(format!(
                    "Malformed configuration: cannot specify both password and key file on host {}",
                    host.address
                )));
            }
            self.hosts.push(host);
        }
        self.tracing_dir = self.tracing_dir.take().or(tracing_dir);
        self.cleanup_executable |= cleanup_executable;
        self.connectors.extend(connectors); // TODO: implement some checks like the ones for the hosts

        Ok(self)
    }

    /// Read toml file and integrate it in the builder.
    /// Hosts are appended to the list, the rest of the parameters set only if they were not present.
    pub fn parse_file(&mut self, toml_path: impl AsRef<Path>) -> Result<&mut Self, ConfigError> {
        let content = std::fs::read_to_string(toml_path)?;
        self.parse_toml_str(&content)
    }

    pub fn add_hosts(&mut self, hosts: &[HostConfig]) -> &mut Self {
        self.hosts.extend_from_slice(hosts);
        self
    }

    /// Read toml from env variable [CONFIG_ENV_VAR] and integrate it in the builder.
    /// Hosts are appended to the list, the rest of the parameters set only if they were not present.
    pub fn parse_env(&mut self) -> Result<&mut Self, ConfigError> {
        let config_str = env::var(CONFIG_ENV_VAR)
            .map_err(|e| ConfigError::Environment(CONFIG_ENV_VAR.to_string(), e))?;
        self.parse_toml_str(&config_str)
    }

    pub fn host_id(&mut self, host_id: HostId) -> &mut Self {
        self.host_id = Some(host_id);
        self
    }

    /// Extract the host id from the environment variable [HOST_ID_ENV_VAR].
    pub fn host_id_from_env(&mut self) -> Result<&mut Self, ConfigError> {
        let host_id = env::var(HOST_ID_ENV_VAR)
            .map_err(|e| ConfigError::Environment(HOST_ID_ENV_VAR.to_string(), e))?;
        let host_id = HostId::from_str(&host_id)
            .map_err(|_| ConfigError::Invalid("host_id must be an integer".into()))?;
        self.host_id = Some(host_id);
        Ok(self)
    }

    pub fn host_tag(&mut self, host_tag: HostTag) -> &mut Self {
        self.host_tag = Some(host_tag);
        self
    }

    /// Extract the host tag from the environment variable [GROUP_TAG_ENV_VAR].
    pub fn host_tag_from_env(&mut self) -> Result<&mut Self, ConfigError> {
        self.host_tag = env::var(HOST_TAG_ENV_VAR).ok();
        log::debug!("On this machine HOST_TAG is {:?}", self.host_tag);
        Ok(self)
    }

    pub fn build(&mut self) -> Result<RuntimeConfig, ConfigError> {
        if let Some(host_id) = self.host_id {
            let num_hosts = self.hosts.len() as u64;
            if host_id >= num_hosts {
                return Err(ConfigError::Invalid(format!(
                    "invalid host_id, must be between 0 and the number of hosts - 1: (0..{num_hosts})",
                )));
            }
        };

        let conf = RuntimeConfig::Remote(RemoteConfig {
            host_id: self.host_id,
            hosts: self.hosts.clone(),
            tracing_dir: self.tracing_dir.clone(),
            cleanup_executable: self.cleanup_executable,
            connectors: self.connectors.clone(),
        });
        Ok(conf)
    }
}

/// Default port for ssh, used by the serde default value.
fn ssh_default_port() -> u16 {
    22
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Serialization error: {0}")]
    Serialization(#[from] toml::de::Error),

    #[error("Input-Output error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Invalid configuration: {0}")]
    Invalid(String),

    #[error("Missing environment variable {0}: {1}")]
    Environment(String, env::VarError),
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct GroupConnector {
    pub from: String,
    pub to: String,
    pub technology: ConnectorTechnology,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type")]
pub enum ConnectorTechnology {
    Kafka(KafkaConfig),
    Redis(RedisConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub timeout: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct RedisConfig {
    pub urls: Vec<String>,
    pub key: String,
}

impl GroupConnector {
    pub fn split<T: ExchangeData>(
        self,
    ) -> (ConnectorSinkTechnology<T>, ConnectorSourceTechnology<T>) {
        match &self.technology {
            ConnectorTechnology::Kafka(kafka) => (
                ConnectorSinkTechnology::Kafka(kafka.into()),
                ConnectorSourceTechnology::Kafka(kafka.into()),
            ),
            ConnectorTechnology::Redis(_redis) => todo!("config -> (sink, source)"),
        }
    }
}

impl From<&DistributedConfig> for RemoteConfig {
    fn from(distributed: &DistributedConfig) -> Self {
        RemoteConfig {
            host_id: distributed.host_id,
            hosts: distributed
                .tiers
                .iter()
                .flat_map(|t| t.hosts.clone())
                .collect(),
            tracing_dir: None,
            cleanup_executable: false,
            connectors: distributed.connectors.clone(),
        }
    }
}
