ssh ubuntu@www 'cp /home/ubuntu/golden-axe/target/release/fe /home/ubuntu/fe.bak'
ssh ubuntu@www 'bash -l -c "cd golden-axe && git pull && cargo build -p fe --release"'
ssh ubuntu@www 'sudo systemctl restart fe'
