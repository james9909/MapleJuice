use crate::heartbeat::HeartbeatStyle;
use std::str::FromStr;

impl FromStr for HeartbeatStyle {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gossip" => Ok(HeartbeatStyle::Gossip),
            "all_to_all" => Ok(HeartbeatStyle::AllToAll),
            _ => Err(format!("invalid gossip style: {}", s)),
        }
    }
}

#[derive(Debug)]
pub enum Command {
    ShowMembershipList,
    WhoAmI,
    JoinGroup,
    LeaveGroup,
    SetStyle(HeartbeatStyle),
}

impl FromStr for Command {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let args: Vec<&str> = s.split(" ").collect();
        if args.is_empty() {
            return Err("command cannot be empty".to_owned());
        }
        match args[0] {
            "list" => Ok(Command::ShowMembershipList),
            "whoami" => Ok(Command::WhoAmI),
            "join" => Ok(Command::JoinGroup),
            "leave" => Ok(Command::LeaveGroup),
            "heartbeat" => {
                if args.len() < 2 {
                    Err("syntax: heartbeat <all_to_all|gossip>".to_owned())
                } else {
                    HeartbeatStyle::from_str(args[1]).map(|style| Command::SetStyle(style))
                }
            }
            _ => Err(format!("no such command: {}.", args[0])),
        }
    }
}
