use std::io::Write;
use std::net::SocketAddr;

use clap::{App, Arg};

use libmj::client::MjClient;
use libmj::request::PartitionStyle;

use libsdfs::client::SdfsClient;

use libutils::anyhow::Result;
use libutils::net::get_socket_addr;
use libutils::tokio;

async fn execute_command(
    args: Vec<&str>,
    mj_client: &mut MjClient,
    sdfs_client: &mut SdfsClient,
) -> Result<()> {
    match args[0] {
        // SDFS client commands
        "put" => {
            if args.len() != 3 {
                println!("Usage: put <localfilename> <sdfsfilename>");
            } else {
                sdfs_client.put(args[1], args[2], false).await?;
            }
        }
        "get" => {
            if args.len() != 3 {
                println!("Usage: get <sdfsfilename> <localfilename>");
            } else {
                sdfs_client.get(args[1], args[2]).await?;
            }
        }
        "delete" => {
            if args.len() != 2 {
                println!("Usage: delete <sdfsfilename>");
            } else {
                sdfs_client.delete(args[1]).await?;
                println!("Deleted file.");
            }
        }
        "ls" => {
            if args.len() != 2 {
                println!("Usage: ls <sdfsfilename>");
            } else {
                let replicas = sdfs_client.list(args[1]).await?;
                if replicas.is_empty() {
                    println!("No such file {}.", args[1]);
                } else {
                    for replica in replicas {
                        println!("{}", replica);
                    }
                }
            }
        }

        // MapleJuice commands
        "maple" => {
            if args.len() != 5 {
                println!("Usage: maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>");
            } else {
                // Upload the exe first
                sdfs_client
                    .put(args[1], args[1], false)
                    .await
                    .expect("failed to upload exe");
                mj_client
                    .submit_maple(
                        args[1],
                        args[2].parse().expect("invalid argument for num_maples"),
                        args[3],
                        args[4],
                    )
                    .await
                    .expect("failed to submit maple task");
            }
        }
        "juice" => {
            if args.len() < 5 {
                println!("Usage: juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={{0,1}} partition={{range,hash}}");
            } else {
                // Upload the exe first
                sdfs_client
                    .put(args[1], args[1], false)
                    .await
                    .expect("failed to upload exe");

                let mut delete_input = false;
                let mut partition = PartitionStyle::Range;
                for i in 5..args.len() {
                    if args[i].starts_with("delete_input") {
                        delete_input = args[i].get(13..).unwrap().parse()?;
                    } else if args[i].starts_with("partition") {
                        match args[i].get(10..).unwrap() {
                            "hash" => {
                                partition = PartitionStyle::Hash;
                            }
                            "range" => {
                                partition = PartitionStyle::Range;
                            }
                            _ => println!("Invalid partition style"),
                        }
                    }
                }
                mj_client
                    .submit_juice(
                        args[1],
                        args[2].parse()?,
                        args[3],
                        args[4],
                        delete_input,
                        partition,
                    )
                    .await?;
            }
        }
        _ => {
            println!("Unrecognized command {}", args[0]);
        }
    };
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("mj-client")
        .version("0.1")
        .author("James Wang <jameswang9909@hotmail.com>, Rishi Desai <desai.rishi1@gmail.com>")
        .about("MJ Client")
        .arg(
            Arg::new("master")
                .short('m')
                .long("master")
                .value_name("MASTER")
                .about("The host for the master")
                .required(true),
        )
        .arg(
            Arg::new("cmd")
                .short('c')
                .long("cmd")
                .value_name("COMMAND")
                .about("Single command to run with client.")
                .required(false),
        )
        .get_matches();
    let master: SocketAddr = get_socket_addr(matches.value_of("master").unwrap())?;

    let mut sdfs_client = SdfsClient::new(SocketAddr::new(master.ip(), master.port() + 1));
    let mut mj_client = MjClient::new(master);
    if let Some(cmd) = matches.value_of("cmd") {
        if let Err(e) = execute_command(
            cmd.trim_end().split(" ").collect(),
            &mut mj_client,
            &mut sdfs_client,
        )
        .await
        {
            println!("Error when executing command: {}", e);
        }
    } else {
        let mut buffer = String::new();
        loop {
            buffer.clear();
            print!("> ");
            std::io::stdout().flush().unwrap();
            std::io::stdin().read_line(&mut buffer).unwrap();
            if let Err(e) = execute_command(
                buffer.trim_end().split(" ").collect(),
                &mut mj_client,
                &mut sdfs_client,
            )
            .await
            {
                println!("Error when executing command: {}", e);
            }
        }
    }
    Ok(())
}
