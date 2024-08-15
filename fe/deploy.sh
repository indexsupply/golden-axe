ssh ubuntu@www 'bash -l -c "cd gafe && git pull && cargo build --release"'
ssh ubuntu@www 'sudo systemctl restart gafe'
