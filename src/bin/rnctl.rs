use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
};

use askama::Template;
use clap::Parser;
use renoir::{
    config::{HostConfig, RemoteConfig, SSHConfig},
    RuntimeConfig,
};
use std::process::Command;

#[derive(Debug, clap::Parser)]
enum Cli {
    Deploy {
        #[clap(
            short,
            long,
            value_name = "CONFIG",
            help = "Path to the configuration file"
        )]
        config: String,
        #[clap(
            short,
            long,
            value_name = "EXECUTABLE",
            help = "Path to the executable to run on remote hosts"
        )]
        executable: Option<String>,
        #[clap(
            short,
            long,
            value_name = "GROUP",
            default_value = "all",
            help = "Group to deploy the executable to (optional, default value is 'all')"
        )]
        group: String,
    },
    Stop {
        group: String,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    check_ansible_installed()?;

    let working_dir = Path::new("/tmp/renoir/ansible");
    if !working_dir.exists() {
        std::fs::create_dir_all(working_dir)?;
    }

    let cmd = Cli::parse();
    match cmd {
        Cli::Deploy { config, executable, group } => {
            // 1 - read config and generate the ansible inventory file
            // 2 - generate the ansible playbook file
            // 3 - run the ansible playbook to deploy the executable on remote hosts

            let config_path = PathBuf::from(config);
            if !config_path.exists() {
                return Err(
                    format!("Configuration file not found at {}", config_path.display()).into(),
                );
            }
            let config = RuntimeConfig::remote(config_path)
                .map_err(|e| format!("Failed to read configuration file: {}", e))?;
            let config = match config {
                RuntimeConfig::Remote(config) => config,
                _ => unreachable!("Expected remote configuration"),
            };
            let inventory = into_inventory(config)
                .map_err(|e| format!("Failed to generate inventory: {}", e))?;

            let inventory_path = working_dir.join("inventory.ini");
            if inventory_path.exists() {
                std::fs::remove_file(&inventory_path)?;
            }
            std::fs::write(&inventory_path, inventory)?;

            let deploy_playbook = include_str!("../../ansible/deploy.yaml");
            let playbook_path = working_dir.join("deploy.yaml");
            if !playbook_path.exists() {
                std::fs::write(&playbook_path, deploy_playbook)?;
            }

            let mut renoir_executable = match executable {
                Some(executable) => PathBuf::from(executable),
                None => {
                    let cwd = std::env::current_dir()?;
                    let cwd = cwd.file_name().ok_or("Where are we?")?;
                    let exe = PathBuf::from(format!("target/release/{}", cwd.to_string_lossy()));
                    if !exe.exists() {
                        return Err(format!("Executable not found at {}", exe.display()).into());
                    }
                    exe
                }
            };
            if !renoir_executable.exists() {
                return Err(format!(
                    "Renoir executable not found at {}",
                    renoir_executable.display()
                )
                .into());
            }
            if !renoir_executable.is_absolute() {
                renoir_executable = std::env::current_dir()?.join(renoir_executable);
            }
            std::env::set_current_dir(working_dir)?;
            let deploy_command = format!(
                "ansible-playbook -i {} {} --extra-vars \"renoir_executable={}\" --extra-vars \"renoir_target_group={}\"",
                inventory_path.display(),
                playbook_path.display(),
                renoir_executable.display(),
                group
            );
            println!("Running deploy command: {}", deploy_command);
            Command::new("sh")
                .arg("-c")
                .arg(deploy_command)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .output()
                .map_err(|e| format!("Failed to run ansible-playbook: {}", e))?;
        }
        Cli::Stop { .. } => todo!(":("),
    }

    Ok(())
}

fn check_ansible_installed() -> Result<(), String> {
    let output = Command::new("ansible").arg("--version").output();
    match output {
        Ok(output) => {
            if output.status.success() {
                // println!("Ansible is installed.");
            } else {
                return Err("Ansible is not installed or not in PATH.".to_string());
            }
        }
        Err(_) => {
            return Err("Ansible is not installed or not in PATH.".to_string());
        }
    }

    let output = Command::new("ansible-playbook").arg("--version").output();
    match output {
        Ok(output) => {
            if output.status.success() {
                // println!("Ansible Playbook is installed.");
            } else {
                return Err("Ansible Playbook is not installed or not in PATH.".to_string());
            }
        }
        Err(_) => {
            return Err("Ansible Playbook is not installed or not in PATH.".to_string());
        }
    }

    Ok(())
}

#[derive(Debug, askama::Template)]
#[template(path = "inventory.template", escape = "none")]
struct InventoryTemplate {
    // TODO: the outer hashmap (layers) could be removed: template needs only the group name
    host_map: HashMap<String, HashMap<String, Vec<String>>>,
    ssh_map: HashMap<String, (String, SSHConfig)>,
    group_map: HashMap<String, Vec<String>>,
}

fn into_inventory(config: RemoteConfig) -> Result<String, String> {
    let RemoteConfig {
        hosts,
        group_connections,
        ..
    } = config;

    let mut host_map: HashMap<String, HashMap<String, Vec<String>>> = Default::default();
    let mut ssh_map: HashMap<String, (String, SSHConfig)> = Default::default();
    for (idx, host) in hosts.into_iter().enumerate() {
        let host_name = format!("host-{}", idx);
        let HostConfig {
            layer,
            group,
            address,
            ssh,
            ..
        } = host;

        let layer = layer.ok_or("Host layer is not specified")?;
        let group = group.ok_or("Host group is not specified")?;
        host_map
            .entry(layer)
            .or_default()
            .entry(group)
            .or_default()
            .push(host_name.clone());
        ssh_map.insert(host_name, (address, ssh));
    }

    let mut group_map: HashMap<String, Vec<String>> = Default::default();
    for conn in group_connections {
        group_map.insert(conn.to, conn.from);
    }

    InventoryTemplate {
        host_map,
        ssh_map,
        group_map,
    }
    .render()
    .map_err(|e| format!("Failed to render inventory template: {}", e))
}

impl InventoryTemplate {
    fn render_ssh(&self, ssh: &Option<&(String, SSHConfig)>) -> String {
        if ssh.is_none() {
            return String::new();
        }
        let (address, ssh) = ssh.unwrap();
        let mut result = Vec::new();
        result.push(format!("ansible_host={}", address));
        result.push(format!("ansible_port={}", ssh.ssh_port));
        if let Some(key_file) = &ssh.key_file {
            result.push(format!(
                "ansible_ssh_private_key_file={}\n",
                key_file.display()
            ));
        }
        if let Some(user) = ssh.username.as_deref() {
            result.push(format!("ansible_user={}\n", user));
        }
        result.join(" ")
    }
}
