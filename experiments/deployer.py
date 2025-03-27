from tomllib import load
from pydantic import BaseModel, Field
from argparse import ArgumentParser
from yaml import dump
import re
import subprocess

ANNOTATION_REGEX = r"(\w+)\s*(=|<|>|<=|>=)\s*(\w+)"


class SSH(BaseModel):
    user: str
    password: str | None = None
    key_file: str | None = None
    port: int | None = 22


class Connection(BaseModel):
    type: str
    topic: str
    brokers: list[str]
    timeout: int | None = None


class Group(BaseModel):
    name: str
    constraints: str | None = None
    input: Connection | None = None
    output: Connection | None = None

    def dump_input(self):
        if self.input:
            return self.input.model_dump()
        return None

    def dump_output(self):
        if self.output:
            return self.output.model_dump()
        return None


class Host(BaseModel):
    name: str
    address: str
    base_port: int
    num_cores: int
    ssh: SSH
    capabilites: dict = Field(default={})
    zone: str | None = None

    def dump_ansible(self):
        return {
            "ansible_host": self.address,
            "renoir_base_port": self.base_port,
            "renoir_num_cores": self.num_cores,
        }


class Heartbeat(BaseModel):
    brokers: list[str]
    topic: str = Field(default="heartbeat")
    timeout: int = Field(default=30)


class InputConfig(BaseModel):
    hosts: list[Host] = Field(alias="host")
    groups: list[Group] = Field(alias="group")
    heartbeat: Heartbeat

    def get_host(self, name: str) -> Host:
        for host in self.hosts:
            if host.name == name:
                return host
        raise ValueError(f"Host {name} not found")

    def get_group(self, name: str) -> Group:
        for group in self.groups:
            if group.name == name:
                return group
        raise ValueError(f"Group {name} not found")


class Annotation:
    def __init__(self, key, op, value):
        self.key = key  # this will be always a string
        self.op = op  # this will be always a string
        try:
            self.value = int(value)
            return
        except ValueError:
            pass
        try:
            self.value = float(value)
            return
        except ValueError:
            pass
        self.value = str(value)

    def __str__(self):
        return f"{self.key} {self.op} {self.value}"

    def is_satisfied(self, value):
        if self.op == "=":
            if isinstance(self.value, str):
                return value == self.value
            elif isinstance(self.value, int):
                return value == self.value
            elif isinstance(self.value, float):
                return value == self.value
            else:
                raise ValueError(f"Invalid value: {self.value}")
        elif self.op == "<":
            if isinstance(self.value, int):
                return value < self.value
            elif isinstance(self.value, float):
                return value < self.value
            else:
                raise ValueError(f"Invalid value: {self.value}")
        elif self.op == ">":
            if isinstance(self.value, int):
                return value > self.value
            elif isinstance(self.value, float):
                return value > self.value
            else:
                raise ValueError(f"Invalid value: {self.value}")
        elif self.op == "<=":
            if isinstance(self.value, int):
                return value <= self.value
            elif isinstance(self.value, float):
                return value <= self.value
            else:
                raise ValueError(f"Invalid value: {self.value}")
        elif self.op == ">=":
            if isinstance(self.value, int):
                return value >= self.value
            elif isinstance(self.value, float):
                return value >= self.value
            else:
                raise ValueError(f"Invalid value: {self.value}")
        else:
            raise ValueError(f"Invalid operator: {self.op}")


def parse_config(filepath):
    with open(filepath, "rb") as f:
        config = load(f)
    return InputConfig(**config)


def parse_annotation(annotation) -> Annotation:
    match = re.match(ANNOTATION_REGEX, annotation)
    if not match:
        raise ValueError(f"Invalid annotation: {annotation}")
    return Annotation(*match.groups())


def host_belong_group(host: Host, annotations: list[Annotation]) -> bool:
    temp = True
    for name, value in host.capabilites.items():
        for annotation in annotations:
            if annotation.key == name:
                temp &= annotation.is_satisfied(value)
        if not temp:
            break
    return temp


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "-ic", "--input_config", help="Path to the input configuration file"
    )
    parser.add_argument(
        "-oi", "--output_inventory", help="Path to the output Ansible inventory file"
    )
    parser.add_argument(
        "-e",
        "--executable",
        help="Path to the executable",
        default="../target/debug/examples/groups",
        required=False,
    )
    parser.add_argument(
        "-tg",
        "--target_group",
        help="Name of the target group",
        default="all",
        required=False,
    )
    parser.add_argument(
        "-ap",
        "--ansible_playbook",
        help="Path to the ansible playbook",
        default="deploy_distributed.yaml",
        required=False,
    )
    parser.add_argument(
        "-ra",
        "--renoir_arguments",
        help="Arguments to pass to the executable",
        default="-d",
        required=False,
    )
    args = parser.parse_args()

    if not re.match(r".+\.toml", args.input_config):
        raise ValueError("Input configuration file must have a .toml extension")

    if not re.match(r".+\.yaml|.+\.yml", args.output_inventory):
        raise ValueError("Output inventory file must have a yaml extension")

    input_config = parse_config(args.input_config)

    # TODO: Handle the case in which a host belongs to multiple groups
    mapping = {}
    for group in input_config.groups:
        if not group.constraints:
            raise ValueError(f"Group {group.name} has no annotations")
        annotations = [parse_annotation(a) for a in group.constraints.split(",")]

        for host in input_config.hosts:
            if host_belong_group(host, annotations):
                mapping.setdefault(group.name, []).append(host.name)

    all_childer = {}

    for map_group, map_hosts in mapping.items():
        group_data = {}
        if isinstance(map_hosts, dict):
            group_children = {}
            for key, value in map_hosts.items():
                group_children[key] = {
                    "hosts": {v: input_config.get_host(v).dump_ansible() for v in value}
                }
            group_data["children"] = group_children

            group_vars = {"renoir_connections": {}}
            for key, value in map_hosts.items():
                group_vars["renoir_connections"][key] = {
                    "input": input_config.get_group(map_group).dump_input(),
                    "output": input_config.get_group(map_group).dump_output(),
                }
            group_data["vars"] = group_vars
        elif isinstance(map_hosts, list):
            group_data["hosts"] = {
                v: input_config.get_host(v).dump_ansible() for v in map_hosts
            }
            group_data["vars"] = {
                "renoir_connections": {
                    "default": {
                        "input": input_config.get_group(map_group).dump_input(),
                        "output": input_config.get_group(map_group).dump_output(),
                    }
                }
            }
        else:
            raise ValueError(f"Invalid mapping type: {map_hosts}")

        all_childer[map_group] = group_data

    with open(args.output_inventory, "w") as f:
        output = {
            "all": {
                "children": all_childer,
                "vars": {
                    "renoir_heartbeat": {
                        "brokers": input_config.heartbeat.brokers,
                        "topic": input_config.heartbeat.topic,
                        "timeout": input_config.heartbeat.timeout,
                    }
                },
            }
        }
        dump(output, f)

    # Run the ansible playbook
    command = [
            "ansible-playbook",
            "-i",
            args.output_inventory,
            args.ansible_playbook,
            "-e",
            f"renoir_executable={args.executable}",
            "-e",
            f"renoir_target_group={args.target_group}",
            "-e",
            f"renoir_arguments='{args.renoir_arguments}'",
        ]
    print(command)
    subprocess.run(command)


if __name__ == "__main__":
    main()
