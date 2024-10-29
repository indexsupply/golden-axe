use clap::Parser;
use eyre::Result;
use itertools::Itertools;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::{fs, thread};

#[derive(Parser)]
struct Args {
    #[arg(required = false)]
    host_filter: Option<String>,
    #[clap(env = "HOST_FILE", default_value = "hosts.txt")]
    host_file: String,
    #[clap(short = 's', action = clap::ArgAction::SetTrue)]
    status_only: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let ga_home = std::env::var("GAHOME").expect("missing GAHOME");
    let servers = fs::read_to_string(Path::new(&format!("{}/{}", &ga_home, &args.host_file)))?
        .lines()
        .map(String::from)
        .filter(|server| {
            args.host_filter
                .as_ref()
                .map_or(true, |hf| server.contains(hf))
        })
        .collect_vec();
    let command = if args.status_only {
        "cd golden-axe && git show --oneline -s"
    } else {
        "bash -l -c 'cd golden-axe && git show --oneline -s && git pull && cargo build --release && sudo systemctl restart ga'"
    };
    let mut handles = Vec::new();
    for server in servers {
        let server = server.to_string();
        let handle = thread::spawn(move || -> Result<()> {
            let mut cmd = Command::new("ssh")
                .arg(format!("ubuntu@{}", &server))
                .arg(command)
                .stdout(Stdio::piped())
                .spawn()?;
            if let Some(stdout) = cmd.stdout.take() {
                let reader = BufReader::new(stdout);
                for line in reader.lines() {
                    println!("{}:\t{}", server, line?);
                }
            }
            cmd.wait()?;
            Ok(())
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().expect("Thread panicked")?;
    }
    Ok(())
}
