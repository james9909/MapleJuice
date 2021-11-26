# MapleJuice

### Build and Run

#### Install rustc and cargo

***Note:  Only necessary if you haven't installed rust.***

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

#### Build all binaries

```
cargo build --release
```

#### How to run MapleJuice?

```
cargo run --release --bin mj-server -- --help

mj-server 0.1
James Wang <jameswang9909@hotmail.com>, Rishi Desai <desai.rishi1@gmail.com>
MapleJuice daemon

USAGE:
    mj-server [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --base_path <BASE_PATH>    Base path for storing files [default: files]
    -l, --log <LOG_PATH>           File to log to (default: stdout)
    -m, --master <MASTER>          The hostname of the master
    -p, --port <PORT>              Base port for all services [default: 3000]
```

Examples:

Start MapleJuice on port 3000

```cargo run --release --bin mj-server -- -p 3000```

Start the server on port 3000, with master 127.0.0.1:3000

```cargo run --release --bin mj-server -- -p 3000 -m 127.0.0.1:3000```

#### How to run the client?

```
cargo run --release --bin mj-client -- --help

mj-client 0.1
James Wang <jameswang9909@hotmail.com>, Rishi Desai <desai.rishi1@gmail.com>
MJ Client

USAGE:
    mj-client [OPTIONS] --master <MASTER>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --cmd <COMMAND>      Single command to run with client.
    -m, --master <MASTER>    The host for the master

```

Examples:

Start the client with master 127.0.0.1:3000

```cargo run --release --bin mj-client -- -m localhost:3000```
